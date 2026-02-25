import json
import pandas as pd
from typing import List, Dict, Any

def check_eligibility_structure(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check if a record has the correct eligibility criteria structure.
    
    Args:
        record: JSON record to check
        
    Returns:
        Dictionary with check results
    """
    result = {
        'nctId': None,
        'has_eligibility_module': False,
        'has_eligibility_criteria': False,
        'is_correct_structure': False,
        'current_type': None,
        'has_inclusion': False,
        'has_exclusion': False,
        'issue': None
    }
    
    try:
        # Extract NCT ID
        protocol_section = record.get('protocolSection', {})
        identification_module = protocol_section.get('identificationModule', {})
        result['nctId'] = identification_module.get('nctId', 'UNKNOWN')
        
        # Check eligibility module
        eligibility_module = protocol_section.get('eligibilityModule', {})
        
        if not eligibility_module:
            result['issue'] = 'No eligibilityModule found'
            return result
        
        result['has_eligibility_module'] = True
        
        # Check eligibility criteria
        if 'eligibilityCriteria' not in eligibility_module:
            result['issue'] = 'No eligibilityCriteria field'
            return result
        
        result['has_eligibility_criteria'] = True
        eligibility_criteria = eligibility_module['eligibilityCriteria']
        
        # Check the type and structure
        if isinstance(eligibility_criteria, str):
            result['current_type'] = 'string'
            result['issue'] = 'eligibilityCriteria is a string (should be object)'
            return result
        
        elif isinstance(eligibility_criteria, dict):
            result['current_type'] = 'object'
            
            # Check if it has the required keys
            has_inclusion = 'inclusionCriteria' in eligibility_criteria
            has_exclusion = 'exclusionCriteria' in eligibility_criteria
            
            result['has_inclusion'] = has_inclusion
            result['has_exclusion'] = has_exclusion
            
            if has_inclusion and has_exclusion:
                # Check if values are strings
                inclusion_val = eligibility_criteria['inclusionCriteria']
                exclusion_val = eligibility_criteria['exclusionCriteria']
                
                if isinstance(inclusion_val, str) and isinstance(exclusion_val, str):
                    result['is_correct_structure'] = True
                    result['issue'] = None
                else:
                    result['issue'] = 'inclusionCriteria or exclusionCriteria is not a string'
            else:
                missing = []
                if not has_inclusion:
                    missing.append('inclusionCriteria')
                if not has_exclusion:
                    missing.append('exclusionCriteria')
                result['issue'] = f'Missing keys: {", ".join(missing)}'
        
        else:
            result['current_type'] = type(eligibility_criteria).__name__
            result['issue'] = f'eligibilityCriteria is {result["current_type"]} (unexpected type)'
    
    except Exception as e:
        result['issue'] = f'Error: {str(e)}'
    
    return result


def process_json_file_streaming(input_file: str, output_excel: str):
    """
    Process JSON file using streaming for memory efficiency.
    
    Args:
        input_file: Path to input JSON file
        output_excel: Path to output Excel file
    """
    print(f"Reading from: {input_file}")
    print("Using streaming mode for memory efficiency...")
    
    try:
        import ijson
    except ImportError:
        print("ERROR: ijson library not found!")
        print("Install with: pip install ijson")
        print("\nFalling back to standard processing...")
        process_json_file_standard(input_file, output_excel)
        return
    
    incorrect_records = []
    correct_count = 0
    total_count = 0
    
    print("Processing records...\n")
    
    try:
        with open(input_file, 'rb') as f:
            parser = ijson.items(f, 'item')
            
            for record in parser:
                total_count += 1
                
                if total_count % 100 == 0:
                    print(f"Processed {total_count} records... (Found {len(incorrect_records)} incorrect)")
                
                check_result = check_eligibility_structure(record)
                
                if not check_result['is_correct_structure']:
                    incorrect_records.append(check_result)
                else:
                    correct_count += 1
    
    except Exception as e:
        print(f"Error during streaming: {e}")
        return
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total records processed: {total_count}")
    print(f"Records with CORRECT structure: {correct_count}")
    print(f"Records with INCORRECT structure: {len(incorrect_records)}")
    print("="*60)
    
    # Create DataFrame and save to Excel
    if incorrect_records:
        df = pd.DataFrame(incorrect_records)
        
        # Reorder columns for better readability
        column_order = [
            'nctId',
            'issue',
            'current_type',
            'has_eligibility_module',
            'has_eligibility_criteria',
            'has_inclusion',
            'has_exclusion',
            'is_correct_structure'
        ]
        
        df = df[column_order]
        
        # Save to Excel
        print(f"\nSaving results to: {output_excel}")
        df.to_excel(output_excel, index=False, sheet_name='Incorrect Structure')
        print(f"✓ Excel file created with {len(incorrect_records)} records")
        
        # Show breakdown by issue type
        print("\nBreakdown by issue type:")
        print("-" * 60)
        issue_counts = df['issue'].value_counts()
        for issue, count in issue_counts.items():
            print(f"  {issue}: {count}")
        
        # Show first few NCT IDs as examples
        print("\nFirst 10 NCT IDs with incorrect structure:")
        print("-" * 60)
        for i, nct_id in enumerate(df['nctId'].head(10), 1):
            issue = df[df['nctId'] == nct_id]['issue'].values[0]
            print(f"  {i}. {nct_id} - {issue}")
    
    else:
        print("\n✓ All records have the correct eligibility criteria structure!")
        # Create empty Excel file with headers
        df = pd.DataFrame(columns=[
            'nctId', 'issue', 'current_type', 'has_eligibility_module',
            'has_eligibility_criteria', 'has_inclusion', 'has_exclusion',
            'is_correct_structure'
        ])
        df.to_excel(output_excel, index=False, sheet_name='Incorrect Structure')
        print(f"Empty Excel file created: {output_excel}")


def process_json_file_standard(input_file: str, output_excel: str):
    """
    Process JSON file using standard method (loads entire file).
    
    Args:
        input_file: Path to input JSON file
        output_excel: Path to output Excel file
    """
    print("Loading JSON data...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return
    
    # Ensure data is a list
    if not isinstance(data, list):
        data = [data]
    
    print(f"Total records: {len(data)}")
    print("Checking eligibility criteria structure...\n")
    
    incorrect_records = []
    correct_count = 0
    
    for idx, record in enumerate(data):
        if idx % 100 == 0 and idx > 0:
            print(f"Processed {idx}/{len(data)} records...")
        
        check_result = check_eligibility_structure(record)
        
        if not check_result['is_correct_structure']:
            incorrect_records.append(check_result)
        else:
            correct_count += 1
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total records processed: {len(data)}")
    print(f"Records with CORRECT structure: {correct_count}")
    print(f"Records with INCORRECT structure: {len(incorrect_records)}")
    print("="*60)
    
    # Create DataFrame and save to Excel
    if incorrect_records:
        df = pd.DataFrame(incorrect_records)
        
        # Reorder columns for better readability
        column_order = [
            'nctId',
            'issue',
            'current_type',
            'has_eligibility_module',
            'has_eligibility_criteria',
            'has_inclusion',
            'has_exclusion',
            'is_correct_structure'
        ]
        
        df = df[column_order]
        
        # Save to Excel
        print(f"\nSaving results to: {output_excel}")
        df.to_excel(output_excel, index=False, sheet_name='Incorrect Structure')
        print(f"✓ Excel file created with {len(incorrect_records)} records")
        
        # Show breakdown by issue type
        print("\nBreakdown by issue type:")
        print("-" * 60)
        issue_counts = df['issue'].value_counts()
        for issue, count in issue_counts.items():
            print(f"  {issue}: {count}")
        
        # Show first few NCT IDs as examples
        print("\nFirst 10 NCT IDs with incorrect structure:")
        print("-" * 60)
        for i, nct_id in enumerate(df['nctId'].head(10), 1):
            issue = df[df['nctId'] == nct_id]['issue'].values[0]
            print(f"  {i}. {nct_id} - {issue}")
    
    else:
        print("\n✓ All records have the correct eligibility criteria structure!")
        # Create empty Excel file with headers
        df = pd.DataFrame(columns=[
            'nctId', 'issue', 'current_type', 'has_eligibility_module',
            'has_eligibility_criteria', 'has_inclusion', 'has_exclusion',
            'is_correct_structure'
        ])
        df.to_excel(output_excel, index=False, sheet_name='Incorrect Structure')
        print(f"Empty Excel file created: {output_excel}")


if __name__ == "__main__":
    input_file = "Last Task/SSP-dev.t2dm_data_final.json"
    output_excel = "Last Task/incorrect_eligibility_structure.xlsx"
    
    process_json_file_streaming(input_file, output_excel)
