import argparse
import re
import sys
from io import BytesIO
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from xhtml2pdf import pisa


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def remove_footer_and_clean(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Remove footer elements and other unnecessary content from the page.
    Common footer selectors: footer, .footer, #footer, .site-footer, etc.
    """
    # Remove footer elements
    footer_selectors = [
        "footer",
        ".footer",
        "#footer",
        ".site-footer",
        "#site-footer",
        ".footer-container",
        "#footer-container",
        "[class*='footer']",
        "[id*='footer']",
        ".site-footer-container",
        "#site-footer-container",
    ]
    
    for selector in footer_selectors:
        for element in soup.select(selector):
            element.decompose()
    
    # Remove common navigation/sidebar elements that aren't main content
    nav_selectors = [
        "nav",
        ".navigation",
        ".sidebar",
        ".menu",
        ".breadcrumb",
        ".skip-link",
    ]
    
    for selector in nav_selectors:
        for element in soup.select(selector):
            # Only remove if it's clearly navigation, not main content navigation
            if "main" not in element.get("class", []) and "content" not in element.get("class", []):
                element.decompose()
    
    # Remove script and style tags
    for element in soup.find_all(["script", "style", "noscript"]):
        element.decompose()
    
    # Remove comments
    from bs4 import Comment
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    return soup


def extract_main_content(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Try to identify and extract the main content area of the page.
    Common main content selectors: main, .main-content, #main-content, article, etc.
    """
    # Try to find main content container
    main_selectors = [
        "main",
        ".main-content",
        "#main-content",
        "article",
        ".content",
        "#content",
        ".page-content",
        "#page-content",
        "[role='main']",
    ]
    
    main_content = None
    for selector in main_selectors:
        main_content = soup.select_one(selector)
        if main_content:
            break
    
    # If main content found, create a new soup with just that
    if main_content:
        new_soup = BeautifulSoup("<html><head></head><body></body></html>", "html.parser")
        new_soup.body.append(main_content)
        return new_soup
    
    # Otherwise, try to remove header/nav and keep body
    body = soup.find("body")
    if body:
        # Remove header if present
        header = body.find("header")
        if header:
            header.decompose()
        
        new_soup = BeautifulSoup("<html><head></head><body></body></html>", "html.parser")
        new_soup.body.extend(body.children)
        return new_soup
    
    return soup


def clean_html_for_pdf(soup: BeautifulSoup) -> str:
    """
    Clean and prepare HTML for PDF conversion.
    Adds basic styling for better PDF output.
    """
    # Add basic CSS for better PDF formatting (xhtml2pdf compatible)
    style_tag = soup.new_tag("style")
    style_tag.string = """
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            padding: 20px;
        }
        h1, h2, h3, h4, h5, h6 {
            color: #2c3e50;
            margin-top: 1.5em;
            margin-bottom: 0.5em;
        }
        h1 { font-size: 2em; }
        h2 { font-size: 1.5em; }
        h3 { font-size: 1.2em; }
        p { margin: 1em 0; }
        ul, ol { margin: 1em 0; padding-left: 2em; }
        li { margin: 0.5em 0; }
        a { color: #3498db; text-decoration: underline; }
        code, pre {
            background-color: #f4f4f4;
            padding: 2px 6px;
            font-family: 'Courier New', monospace;
        }
        pre {
            padding: 10px;
        }
        blockquote {
            padding-left: 1em;
            margin: 1em 0;
            color: #555;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #3498db;
            color: white;
        }
        img {
            max-width: 100%;
        }
    """
    
    head = soup.find("head")
    if not head:
        head = soup.new_tag("head")
        soup.html.insert(0, head)
    head.append(style_tag)
    
    # Ensure charset
    meta_charset = soup.find("meta", charset=True)
    if not meta_charset:
        meta_charset = soup.new_tag("meta", charset="utf-8")
        head.insert(0, meta_charset)
    
    return str(soup)


def scrape_page_to_pdf(url: str, output_pdf: str) -> None:
    """
    Scrape a webpage, clean it, and convert to PDF.
    """
    print(f"Fetching page: {url}")
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception as e:
        print(f"ERROR: Failed to fetch page: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("Parsing HTML...")
    soup = BeautifulSoup(resp.text, "html.parser")
    
    print("Removing footer and cleaning...")
    soup = remove_footer_and_clean(soup)
    
    print("Extracting main content...")
    soup = extract_main_content(soup)
    
    print("Preparing HTML for PDF...")
    html_content = clean_html_for_pdf(soup)
    
    print(f"Generating PDF: {output_pdf}")
    try:
        # Convert HTML to PDF using xhtml2pdf
        with open(output_pdf, "w+b") as pdf_file:
            pisa_status = pisa.CreatePDF(
                html_content,
                dest=pdf_file,
                encoding="utf-8",
            )
        
        if pisa_status.err:
            print(f"WARNING: Some errors occurred during PDF generation", file=sys.stderr)
        else:
            print(f"✓ PDF saved successfully: {output_pdf}")
    except Exception as e:
        print(f"ERROR: Failed to generate PDF: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape a webpage (excluding footer) and convert to PDF"
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
    
    args = parser.parse_args()
    
    # Generate output filename if not provided
    if not args.output:
        parsed = urlparse(args.url)
        # Use the last part of the path as filename
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            filename = path_parts[-1] + ".pdf"
        else:
            filename = "page.pdf"
        args.output = filename
    
    scrape_page_to_pdf(args.url, args.output)


if __name__ == "__main__":
    main()
