import argparse
import base64
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def setup_chrome_driver(headless: bool = True):
    """
    Setup Chrome driver with stealth options to bypass bot detection.
    """
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless=new")
    
    # Stealth mode - hide automation indicators
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Options for better PDF generation
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--enable-print-browser")
    
    # User agent
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
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
        
        # Execute stealth scripts to hide webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        
        return driver
    except Exception as e:
        print(f"ERROR: Failed to initialize Chrome driver: {e}", file=sys.stderr)
        print("\nMake sure you have Chrome installed and chromedriver in your PATH.", file=sys.stderr)
        sys.exit(1)


def remove_unwanted_elements(driver):
    """
    Remove footer, header, navigation and other unnecessary elements using JavaScript.
    """
    # Remove footer elements
    footer_scripts = [
        "document.querySelectorAll('footer').forEach(el => el.remove());",
        "document.querySelectorAll('.footer').forEach(el => el.remove());",
        "document.querySelectorAll('#footer').forEach(el => el.remove());",
        "document.querySelectorAll('.site-footer').forEach(el => el.remove());",
        "document.querySelectorAll('[class*=\"footer\"]').forEach(el => el.remove());",
        "document.querySelectorAll('[id*=\"footer\"]').forEach(el => el.remove());",
    ]
    
    # Remove header/navigation
    nav_scripts = [
        "document.querySelectorAll('header').forEach(el => el.remove());",
        "document.querySelectorAll('nav').forEach(el => el.remove());",
        "document.querySelectorAll('.breadcrumb').forEach(el => el.remove());",
        "document.querySelectorAll('.skip-link').forEach(el => el.remove());",
    ]
    
    all_scripts = footer_scripts + nav_scripts
    
    for script in all_scripts:
        try:
            driver.execute_script(script)
        except Exception:
            pass
    
    # Add CSS to hide footer/header if JavaScript removal didn't work
    hide_css = """
    footer, .footer, #footer, header, nav, .breadcrumb, .skip-link,
    [class*="footer"], [id*="footer"] {
        display: none !important;
    }
    """
    driver.execute_script(f"""
        var style = document.createElement('style');
        style.textContent = `{hide_css}`;
        document.head.appendChild(style);
    """)


def scrape_to_pdf(url: str, output_pdf: str, wait_time: int = 5) -> None:
    """
    Scrape a webpage using browser automation and save as text-selectable PDF.
    Preserves the full visual appearance of the page.
    """
    print(f"\n{'='*60}")
    print(f"Advanced Web Scraper to PDF")
    print(f"{'='*60}")
    print(f"URL:    {url}")
    print(f"Output: {output_pdf}")
    print(f"{'='*60}\n")
    
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
        print(f"Waiting {wait_time} seconds for dynamic content...")
        time.sleep(wait_time)
        
        # Scroll to load lazy images
        print("Scrolling to load images...")
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
        except Exception:
            pass
        
        print("Removing footer and unnecessary elements...")
        remove_unwanted_elements(driver)
        
        # Small delay after removing elements
        time.sleep(0.5)
        
        print(f"Generating PDF: {output_pdf}")
        
        # Use Chrome's print-to-PDF feature via CDP (Chrome DevTools Protocol)
        try:
            print_options = {
                "printBackground": True,
                "paperWidth": 8.5,
                "paperHeight": 11,
                "marginTop": 0.5,
                "marginBottom": 0.5,
                "marginLeft": 0.5,
                "marginRight": 0.5,
                "displayHeaderFooter": False,
                "preferCSSPageSize": True,
            }
            
            # Use Chrome DevTools Protocol to print to PDF
            result = driver.execute_cdp_cmd("Page.printToPDF", print_options)
            
            # Decode base64 PDF data
            pdf_data = base64.b64decode(result["data"])
            
            # Save to file
            with open(output_pdf, "wb") as f:
                f.write(pdf_data)
            
            file_size = len(pdf_data) / 1024
            print(f"✓ PDF created successfully: {output_pdf}")
            print(f"  File size: {file_size:.2f} KB")
            print("  (Text is selectable and copyable)")
            
        except Exception as e:
            print(f"ERROR: Failed to generate PDF via CDP: {e}", file=sys.stderr)
            print("Falling back to alternative method...", file=sys.stderr)
            
            try:
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
    
    print("\n✓ Done! Your PDF is ready for download.")


def main():
    parser = argparse.ArgumentParser(
        description="Advanced webpage scraper - converts webpages to high-quality PDFs"
    )
    parser.add_argument(
        "--url",
        "-u",
        required=True,
        help="URL of the webpage to scrape",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output PDF file path (default: auto-generated from URL)",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=5,
        help="Wait time in seconds for page to fully load (default: 5)",
    )
    
    args = parser.parse_args()
    
    # Generate output filename if not provided
    if not args.output:
        parsed = urlparse(args.url)
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            filename = path_parts[-1].replace(".html", "")
            if not filename:
                filename = path_parts[-2] if len(path_parts) > 1 else "page"
        else:
            filename = parsed.netloc.replace(".org", "").replace("www.", "")
        
        filename = filename.replace("/", "_") + ".pdf"
        args.output = filename
    
    scrape_to_pdf(args.url, args.output, args.wait)


if __name__ == "__main__":
    main()
