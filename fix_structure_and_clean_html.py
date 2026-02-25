import json
import pandas as pd
import re
import html
from typing import Dict, Any, Set
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def clean_html_and_entities(text: str) -> str:
    """
    Remove HTML tags and convert HTML entities to proper characters.
    
    Args:
        text: Text that may contain HTML tags and entities
        
    Returns:
        Cleaned text without HTML tags and with decoded entities
    """
    if not text or not isinstance(text, str):
        return text
    
    # Step 1: Remove HTML tags (e.g., <p>, <br>, <ul>, <li>, etc.)
    text = re.sub(r'<[^>]+>', '', text)
    
    # Step 2: Decode HTML entities (e.g., &lt; -> <, &gt; -> >, &amp; -> &)
    text = html.unescape(text)
    
    # Step 3: Handle escaped HTML entities from JSON
    text = text.replace('\\<', '<')
    text = text.replace('\\>', '>')
    text = text.replace('\\&', '&')
    
    # Step 4: Clean up extra whitespace
    text = re.sub(r' +', ' ', text)
    
    # Step 5: Clean up multiple newlines (keep max 2 consecutive)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    # Step 6: Remove leading/trailing whitespace from each line
    lines = text.split('\n')
    lines = [line.strip() for line in lines]
    text = '\n'.join(lines)
    
    # Step 7: Remove leading/trailing whitespace
    text = text.strip()
    
    return text


def parse_eligibility_criteria(criteria_text: str) -> Dict[str, str]:
    """
    Parse eligibility criteria text and split into inclusion and exclusion criteria.
    Also cleans HTML tags and entities.
    
    Args:
        criteria_text: Raw eligibility criteria string
        
    Returns:
        Dictionary with 'inclusionCriteria' and 'exclusionCriteria' keys
    """
    if not criteria_text or not isinstance(criteria_text, str):
        return {
            "inclusionCriteria": "",
            "exclusionCriteria": ""
        }
    
    # Common patterns to identify inclusion/exclusion sections
    inclusion_patterns = [
        r'Inclusion Criteria:',
        r'INCLUSION CRITERIA:',
        r'Inclusion criteria:',
        r'inclusion criteria:',
        r'Inclusion Criterion:',
        r'INCLUSION CRITERION:',
    ]
    
    exclusion_patterns = [
        r'Exclusion Criteria:',
        r'EXCLUSION CRITERIA:',
        r'Exclusion criteria:',
        r'exclusion criteria:',
        r'Exclusion Criterion:',
        r'EXCLUSION CRITERION:',
    ]
    
    # Find inclusion section
    inclusion_match = None
    for pattern in inclusion_patterns:
        match = re.search(pattern, criteria_text, re.IGNORECASE)
        if match:
            inclusion_match = match
            break
    
    # Find exclusion section
    exclusion_match = None
    for pattern in exclusion_patterns:
        match = re.search(pattern, criteria_text, re.IGNORECASE)
        if match:
            exclusion_match = match
            break
    
    inclusion_text = ""
    exclusion_text = ""
    
    if inclusion_match and exclusion_match:
        # Both sections found
        inclusion_start = inclusion_match.end()
        exclusion_start = exclusion_match.start()
        
        if inclusion_start < exclusion_start:
            # Normal order: Inclusion first, then Exclusion
            inclusion_text = criteria_text[inclusion_start:exclusion_start].strip()
            exclusion_text = criteria_text[exclusion_match.end():].strip()
        else:
            # Reverse order: Exclusion first, then Inclusion
            exclusion_text = criteria_text[exclusion_match.end():inclusion_match.start()].strip()
            inclusion_text = criteria_text[inclusion_start:].strip()
    
    elif inclusion_match:
        # Only inclusion found
        inclusion_text = criteria_text[inclusion_match.end():].strip()
    
    elif exclusion_match:
        # Only exclusion found
        exclusion_text = criteria_text[exclusion_match.end():].strip()
    
    else:
        # No clear sections found - put everything in inclusion
        inclusion_text = criteria_text.strip()
    
    # Clean HTML from both texts
    inclusion_text = clean_html_and_entities(inclusion_text)
    exclusion_text = clean_html_and_entities(exclusion_text)
    
    return {
        "inclusionCriteria": inclusion_text,
        "exclusionCriteria": exclusion_text
    }


def fix_eligibility_structure(record: Dict[str, Any], nct_id: str) -> tuple[Dict[str, Any], bool]:
    """
    Fix the eligibility criteria structure for a record and clean HTML.
    
    Args:
        record: JSON record to fix
        nct_id: NCT ID for logging
        
    Returns:
        Tuple of (fixed_record, was_fixed)
    """
    was_fixed = False
    
    try:
        protocol_section = record.get('protocolSection', {})
        eligibility_module = protocol_section.get('eligibilityModule', {})
        
        if 'eligibilityCriteria' in eligibility_module:
            criteria = eligibility_module['eligibilityCriteria']
            
            # Check if it's a string (needs fixing)
            if isinstance(criteria, str):
                # Parse, restructure, and clean HTML
                parsed_criteria = parse_eligibility_criteria(criteria)
                eligibility_module['eligibilityCriteria'] = parsed_criteria
                was_fixed = True
            
            # Check if it's a dict but missing keys or has HTML
            elif isinstance(criteria, dict):
                has_inclusion = 'inclusionCriteria' in criteria
                has_exclusion = 'exclusionCriteria' in criteria
                
                if not has_inclusion or not has_exclusion:
                    # Add missing keys with empty strings
                    if not has_inclusion:
                        criteria['inclusionCriteria'] = ""
                    if not has_exclusion:
                        criteria['exclusionCriteria'] = ""
                    was_fixed = True
                
                # Clean HTML from both fields
                if 'inclusionCriteria' in criteria:
                    original = criteria['inclusionCriteria']
                    if original:
                        cleaned = clean_html_and_entities(original)
                        if cleaned != original:
                            criteria['inclusionCriteria'] = cleaned
                            was_fixed = True
                
                if 'exclusionCriteria' in criteria:
                    original = criteria['exclusionCriteria']
                    if original:
                        cleaned = clean_html_and_entities(original)
                        if cleaned != original:
                            criteria['exclusionCriteria'] = cleaned
                            was_fixed = True
    
    except Exception as e:
        print(f"  Error fixing {nct_id}: {e}")
    
    return record, was_fixed


def process_with_streaming(json_file: str, excel_file: str, output_file: str):
    """
    Process JSON file using streaming for memory efficiency.
    Fixes structure AND cleans HTML in one pass.
    
    Args:
        json_file: Path to input JSON file
        excel_file: Path to Excel file with incorrect NCT IDs
        output_file: Path to output corrected JSON file
    """
    print("="*60)
    print("ELIGIBILITY CRITERIA FIXER (STRUCTURE + HTML CLEANING)")
    print("="*60)
    
    try:
        import ijson
    except ImportError:
        print("\nERROR: ijson library not found!")
        print("Install with: pip install ijson")
        return
    
    # Step 1: Read incorrect NCT IDs from Excel
    print(f"\n1. Reading incorrect NCT IDs from: {excel_file}")
    try:
        df = pd.read_excel(excel_file)
        incorrect_nct_ids = set(df['nctId'].tolist())
        print(f"   Found {len(incorrect_nct_ids)} NCT IDs to fix")
    except Exception as e:
        print(f"   ERROR reading Excel file: {e}")
        return
    
    # Step 2: Process with streaming
    print(f"\n2. Processing JSON file with streaming: {json_file}")
    print(f"   Writing to: {output_file}")
    print(f"   - Fixing structure (string -> object)")
    print(f"   - Cleaning HTML tags and entities")
    
    fixed_count = 0
    skipped_count = 0
    total_count = 0
    
    try:
        with open(json_file, 'rb') as input_f:
            with open(output_file, 'w', encoding='utf-8') as output_f:
                # Start JSON array
                output_f.write('[\n')
                
                parser = ijson.items(input_f, 'item')
                first_item = True
                
                for record in parser:
                    total_count += 1
                    
                    if total_count % 500 == 0:
                        print(f"   Processed {total_count} records... (Fixed: {fixed_count})")
                    
                    # Get NCT ID
                    try:
                        nct_id = record.get('protocolSection', {}).get('identificationModule', {}).get('nctId', 'UNKNOWN')
                    except:
                        nct_id = 'UNKNOWN'
                    
                    # Check if this NCT ID needs fixing
                    if nct_id in incorrect_nct_ids:
                        record, was_fixed = fix_eligibility_structure(record, nct_id)
                        if was_fixed:
                            fixed_count += 1
                    else:
                        skipped_count += 1
                    
                    # Write to output
                    if not first_item:
                        output_f.write(',\n')
                    else:
                        first_item = False
                    
                    json.dump(record, output_f, ensure_ascii=False, cls=DecimalEncoder)
                
                # Close JSON array
                output_f.write('\n]')
        
        print(f"\n   ✓ Processing complete!")
        
    except Exception as e:
        print(f"   ERROR during streaming: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total records processed:        {total_count}")
    print(f"Records fixed:                  {fixed_count}")
    print(f"Records skipped (correct):      {skipped_count}")
    print(f"NCT IDs in Excel:               {len(incorrect_nct_ids)}")
    print("="*60)
    
    if fixed_count > 0:
        print(f"\n✓ Successfully fixed {fixed_count} records!")
        print(f"✓ Structure corrected + HTML cleaned")
        print(f"✓ Output saved to: {output_file}")
    else:
        print("\n⚠ No records were fixed. Please check the data.")


if __name__ == "__main__":
    json_file = "Last Task/SSP-dev.t2dm_data_preprocessed(complete).json"
    excel_file = "Last Task/incorrect_eligibility_structure.xlsx"
    output_file = "Last Task/SSP-dev.t2dm_data_final.json"
    
    # Use streaming for large files
    process_with_streaming(json_file, excel_file, output_file)
