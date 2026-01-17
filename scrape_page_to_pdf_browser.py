import argparse
import base64
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def setup_chrome_driver(headless: bool = True):
    """
    Setup Chrome driver with options for PDF printing.
    """
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless")
    
    # Options for better PDF generation
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Enable print-to-PDF
    chrome_options.add_argument("--enable-print-browser")
    
    # Set preferences for print-to-PDF
    print_prefs = {
        "printing.print_preview_sticky_settings.appState": base64.b64encode(
            b'{"version":2,"isGcpPromoDismissed":false}'
        ).decode(),
        "savefile.default_directory": str(Path.cwd()),
    }
    chrome_options.add_experimental_option("prefs", print_prefs)
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"ERROR: Failed to initialize Chrome driver: {e}", file=sys.stderr)
        print("\nMake sure you have Chrome installed and chromedriver in your PATH.", file=sys.stderr)
        print("You can install chromedriver via: pip install webdriver-manager", file=sys.stderr)
        print("Or download from: https://chromedriver.chromium.org/", file=sys.stderr)
        sys.exit(1)


def remove_footer_via_javascript(driver):
    """
    Remove footer and other unnecessary elements using JavaScript.
    """
    # Remove footer elements
    footer_scripts = [
        "document.querySelectorAll('footer').forEach(el => el.remove());",
        "document.querySelectorAll('.footer').forEach(el => el.remove());",
        "document.querySelectorAll('#footer').forEach(el => el.remove());",
        "document.querySelectorAll('.site-footer').forEach(el => el.remove());",
        "document.querySelectorAll('#site-footer').forEach(el => el.remove());",
        "document.querySelectorAll('.footer-container').forEach(el => el.remove());",
        "document.querySelectorAll('#footer-container').forEach(el => el.remove());",
        "document.querySelectorAll('[class*=\"footer\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[id*=\"footer\"]').forEach(el => el.remove());",
        "document.querySelectorAll('.site-footer-container').forEach(el => el.remove());",
        "document.querySelectorAll('#site-footer-container').forEach(el => el.remove());",
    ]
    
    # Remove navigation/sidebar (but keep main nav if it's part of content)
    nav_scripts = [
        "document.querySelectorAll('nav:not([role=\"main\"]):not(.main-nav)').forEach(el => el.remove());",
        "document.querySelectorAll('.skip-link').forEach(el => el.remove());",
    ]
    
    # Remove feedback/social sharing elements that aren't main content
    other_scripts = [
        "document.querySelectorAll('[class*=\"feedback\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[class*=\"social-share\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[class*=\"print-button\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[data-action=\"print\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[data-action=\"share\"]').forEach(el => el.remove());",
    ]
    
    all_scripts = footer_scripts + nav_scripts + other_scripts
    
    for script in all_scripts:
        try:
            driver.execute_script(script)
        except Exception:
            pass  # Ignore errors if element doesn't exist
    
    # Add CSS to hide footer if JavaScript removal didn't work
    hide_css = """
    footer, .footer, #footer, .site-footer, #site-footer,
    .footer-container, #footer-container,
    [class*="footer"], [id*="footer"],
    .site-footer-container, #site-footer-container {
        display: none !important;
    }
    """
    driver.execute_script(f"""
        var style = document.createElement('style');
        style.textContent = `{hide_css}`;
        document.head.appendChild(style);
    """)


def scrape_page_to_pdf_browser(url: str, output_pdf: str, wait_time: int = 3) -> None:
    """
    Scrape a webpage using browser automation and save as text-selectable PDF.
    """
    print(f"Initializing browser...")
    driver = setup_chrome_driver(headless=True)
    
    try:
        print(f"Loading page: {url}")
        driver.get(url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Additional wait for dynamic content
        time.sleep(wait_time)
        
        print("Removing footer and unnecessary elements...")
        remove_footer_via_javascript(driver)
        
        # Small delay after removing elements
        time.sleep(0.5)
        
        print(f"Generating PDF: {output_pdf}")
        
        # Use Chrome's print-to-PDF feature via CDP (Chrome DevTools Protocol)
        # This creates text-selectable PDFs
        try:
            # Get the page source and use print command
            print_options = {
                "printBackground": True,
                "paperWidth": 8.5,
                "paperHeight": 11,
                "marginTop": 0.4,
                "marginBottom": 0.4,
                "marginLeft": 0.4,
                "marginRight": 0.4,
            }
            
            # Use Chrome DevTools Protocol to print to PDF
            result = driver.execute_cdp_cmd("Page.printToPDF", print_options)
            
            # Decode base64 PDF data
            pdf_data = base64.b64decode(result["data"])
            
            # Save to file
            with open(output_pdf, "wb") as f:
                f.write(pdf_data)
            
            print(f"✓ PDF saved successfully: {output_pdf}")
            print("  (Text is selectable and copyable)")
            
        except Exception as e:
            print(f"ERROR: Failed to generate PDF via CDP: {e}", file=sys.stderr)
            print("Falling back to alternative method...", file=sys.stderr)
            
            # Fallback: Use Selenium's save_screenshot or try window.print()
            # This is less ideal but might work
            try:
                # Try to trigger print dialog and capture
                driver.execute_script("window.print();")
                time.sleep(2)
                print("NOTE: Please save the PDF manually from the print dialog.", file=sys.stderr)
            except Exception as e2:
                print(f"ERROR: Fallback also failed: {e2}", file=sys.stderr)
                sys.exit(1)
    
    except Exception as e:
        print(f"ERROR: Failed to process page: {e}", file=sys.stderr)
        sys.exit(1)
    
    finally:
        driver.quit()
        print("Browser closed.")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape a webpage using browser automation and convert to text-selectable PDF"
    )
    parser.add_argument(
        "--url",
        "-u",
        required=True,
        help="URL of the webpage to scrape, e.g. https://www.cdc.gov/malaria/hcp/clinical-guidance/index.html",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output PDF file path (default: auto-generated from URL)",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=3,
        help="Wait time in seconds for page to fully load (default: 3)",
    )
    
    args = parser.parse_args()
    
    # Generate output filename if not provided
    if not args.output:
        parsed = urlparse(args.url)
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            filename = path_parts[-1].replace(".html", "") + ".pdf"
        else:
            filename = "page.pdf"
        args.output = filename
    
    scrape_page_to_pdf_browser(args.url, args.output, args.wait)


if __name__ == "__main__":
    main()
