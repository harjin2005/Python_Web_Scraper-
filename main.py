import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
from time import sleep
import re
import logging
import argparse

# logging to check errors 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_page(url, retries=3, delay=2):
    """Fetch a webpage with retry logic."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Successfully fetched {url}")
            return response.text
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt + 1 == retries:
                logger.error(f"Failed to fetch {url} after {retries} attempts")
                return None
            sleep(delay)
    return None

def clean_text(text):
    """Clean text by removing extra whitespace and handling missing values."""
    if not text:
        return "N/A"
    text = re.sub(r'\s+', ' ', text.strip())
    return text if text else "N/A"

def analyze_structure(soup):
    """Analyze HTML to detect potential content blocks and tags."""
    # Common container tags
    container_tags = ['article', 'div', 'section', 'li']
    containers = []
    for tag in container_tags:
        elements = soup.find_all(tag)
        for element in elements:
            # Count relevant child tags
            child_tags = element.find_all(['h1', 'h2', 'h3', 'p', 'time'])
            if len(child_tags) > 0:
                containers.append((tag, element))
    
    content_tags = ['h1', 'h2', 'h3', 'p', 'time']
    tag_info = []
    for tag in content_tags:
        elements = soup.find_all(tag)
        if elements:
            sample = clean_text(elements[0].text)[:50] if elements else "N/A"
            tag_info.append({
                'type': tag.upper(),  # e.g., H2, P, TIME
                'tag': tag,
                'count': len(elements),
                'sample': sample
            })
    
    # Filter for tags with significant counts
    tag_info = [info for info in tag_info if info['count'] > 0]
    logger.info(f"Detected {len(tag_info)} content types: {[info['type'] for info in tag_info]}")
    return containers, tag_info

def present_options(tag_info):
    """Present detected content types to the user and get selections."""
    if not tag_info:
        print("No content types detected. Try another URL.")
        logger.error("No content types detected")
        return []
    
    print("\nFound the following content types:")
    for i, info in enumerate(tag_info, 1):
        print(f"{i}. {info['type']} ({info['count']} found, e.g., '{info['sample']}...')")
    
    while True:
        try:
            choices = input("\nEnter numbers to extract (e.g., '1,2' or '1' for single item, or 'all'): ")
            if choices.lower() == 'all':
                return list(range(1, len(tag_info) + 1))
            selected = [int(x) for x in choices.split(',') if x.strip()]
            if all(1 <= x <= len(tag_info) for x in selected):
                return selected
            print("Invalid selection. Please enter numbers within the range.")
        except ValueError:
            print("Please enter valid numbers separated by commas or 'all'.")
    
def scrape_page(soup, containers, tag_info, selected_indices):
    """Extract selected data from a page."""
    data = []
    # Use the first container type with the most relevant children
    container_tag = containers[0][0] if containers else 'article'
    articles = soup.find_all(container_tag)
    
    # Map selected indices to tags and column names
    selected_tags = [(tag_info[i-1]['tag'], tag_info[i-1]['type']) for i in selected_indices]
    
    for article in articles:
        row = {}
        try:
            for tag, col_name in selected_tags:
                element = article.find(tag)
                row[col_name] = clean_text(element.text if element else None)
            data.append(row)
        except AttributeError:
            logger.warning("Skipping article due to missing elements")
            continue
    
    logger.info(f"Scraped {len(data)} items from page")
    return data

def get_next_page(soup, base_url):
    """Find the next page URL for pagination."""
    next_button = soup.find('a', class_=lambda x: x and 'next' in x.lower()) or \
                  soup.find('a', text=re.compile(r'next|page \d+', re.I))
    if next_button and 'href' in next_button.attrs:
        return urljoin(base_url, next_button['href'])
    return None

def main(url, max_pages=3):
    """Main function to run the scraper."""
    # Validate URL
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    # Fetch and analyze first page
    html = fetch_page(url)
    if not html:
        print("Failed to fetch the page. Check the URL or try again.")
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    containers, tag_info = analyze_structure(soup)
    if not containers:
        print("No content containers found. Try another URL.")
        logger.error("No content containers found")
        return
    
    # Present options and get user selections
    selected_indices = present_options(tag_info)
    if not selected_indices:
        print("No data selected. Exiting.")
        return
    
    # Scrape pages
    all_data = []
    current_url = url
    for page_num in range(max_pages):
        logger.info(f"Scraping page {page_num + 1}: {current_url}")
        html = fetch_page(current_url)
        if not html:
            break
        soup = BeautifulSoup(html, 'html.parser')
        page_data = scrape_page(soup, containers, tag_info, selected_indices)
        all_data.extend(page_data)
        current_url = get_next_page(soup, base_url)
        if not current_url:
            logger.info("No more pages to scrape")
            break
        sleep(1)  
    
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates().replace('', 'N/A').fillna('N/A')
        output_file = f"{parsed_url.netloc.replace('.', '_')}_data.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} items to {output_file}")
        print(f"Saved {len(df)} items to {output_file}")
    else:
        print("No data scraped. Try different selections or URL.")
        logger.warning("No data scraped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flexible web scraper for any website")
    parser.add_argument('--url', type=str, help="Website URL to scrape")
    parser.add_argument('--pages', type=int, default=3, help="Maximum number of pages to scrape")
    args = parser.parse_args()
    
    try:
        if args.url:
            main(args.url, args.pages)
        else:
            url = input("Enter website URL (e.g., https://www.bbc.com/news): ")
            max_pages = input("Max pages to scrape (default 3, press Enter to use default): ")
            max_pages = int(max_pages) if max_pages.strip() else 3
            main(url, max_pages)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print("An error occurred. Check scraper.log for details.")