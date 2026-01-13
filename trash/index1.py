import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random

# Configuration
BASE_URL = "https://www.hotnewhiphop.com/articles/news/"
START_PAGE = 13818  # Start from page 1
END_PAGE = 13918  # Adjust based on how many pages exist
OUTPUT_FILE = "hotnewhiphop_articles.xlsx"

# Date range filter
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

def parse_date(date_string):
    """Parse date from the article timestamp"""
    try:
        # Example format: "2010-04-17 01:25:03"
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        return date_obj
    except:
        return None

def is_date_in_range(date_string):
    """Check if date is within the specified range"""
    date_obj = parse_date(date_string)
    if date_obj:
        return START_DATE <= date_obj <= END_DATE
    return False

def scrape_page(page_num):
    """Scrape a single page and return list of articles"""
    url = f"{BASE_URL}{page_num}"
    articles = []
    
    try:
        print(f"Scraping page {page_num}...")
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all article title links (they have 'font-extrabold' class)
        article_links = soup.find_all('a', class_=lambda x: x and 'font-extrabold' in str(x))
        
        # Find all timestamps
        timestamps = soup.find_all('time', class_='client-timestamp')
        
        print(f"  Found {len(article_links)} article links and {len(timestamps)} timestamps")
        
        # If no articles found, might be end of pages
        if len(article_links) == 0:
            return articles, True, False  # articles, page_empty, reached_old_date
        
        has_old_articles = False
        
        # Match links with timestamps
        for i, link in enumerate(article_links):
            article_title = link.get_text(strip=True)
            article_url = link.get('href', '')
            
            # Make URL absolute if relative
            if article_url and not article_url.startswith('http'):
                article_url = f"https://www.hotnewhiphop.com{article_url}"
            
            # Match with corresponding timestamp
            if i < len(timestamps):
                date_str = timestamps[i].get('data-date', '')
                
                if date_str:
                    date_obj = parse_date(date_str)
                    
                    # Check if date is in range
                    if is_date_in_range(date_str):
                        articles.append({
                            'Article URL': article_url,
                            'Article Title': article_title,
                            'Website Address': 'https://www.hotnewhiphop.com',
                            'Date': date_str
                        })
                        print(f"  ✓ {article_title[:60]}... ({date_str[:10]})")
                    
                    # Check if we've gone past our date range
                    elif date_obj and date_obj < START_DATE:
                        has_old_articles = True
        
        # Return: articles found, page not empty, has articles before 2024
        return articles, False, has_old_articles
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error: {e}")
        return articles, False, False
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        return articles, False, False

def main():
    """Main scraping function"""
    all_articles = []
    total_articles = 0
    consecutive_empty = 0
    pages_with_old_articles = 0
    
    print("=" * 70)
    print("HOTNEWHIPHOP SCRAPER")
    print("=" * 70)
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"Pages: {START_PAGE} to {END_PAGE}")
    print("=" * 70 + "\n")
    
    for page in range(START_PAGE, END_PAGE + 1):
        articles, page_empty, has_old_articles = scrape_page(page)
        
        if articles:
            all_articles.extend(articles)
            total_articles += len(articles)
            consecutive_empty = 0
            print(f"  📊 Page {page}: +{len(articles)} articles | Total: {total_articles}\n")
        else:
            if page_empty:
                consecutive_empty += 1
                print(f"  ⚪ Page {page}: Empty page (might be end of site)\n")
            else:
                print(f"  ⚪ Page {page}: No articles in date range\n")
        
        # Track pages with old articles
        if has_old_articles:
            pages_with_old_articles += 1
            print(f"  ⏰ Page {page}: Contains articles before 2024\n")
        else:
            pages_with_old_articles = 0  # Reset counter
        
        # Stop if 5 consecutive pages with only old articles (we've passed our date range)
        if pages_with_old_articles >= 5:
            print("=" * 70)
            print("✓ REACHED 5 CONSECUTIVE PAGES WITH ONLY PRE-2024 ARTICLES - STOPPING")
            print("=" * 70)
            break
        
        # Stop if 10 consecutive empty pages (likely reached end of site)
        if consecutive_empty >= 10:
            print("=" * 70)
            print("⚠ 10 CONSECUTIVE EMPTY PAGES - LIKELY END OF SITE")
            print("=" * 70)
            break
        
        # Save backup every 50 pages
        if page % 50 == 0 and all_articles:
            df_backup = pd.DataFrame(all_articles)
            df_backup = df_backup.drop_duplicates(subset=['Article URL'])
            backup_file = f"backup_page_{page}.xlsx"
            df_backup.to_excel(backup_file, index=False)
            print(f"  💾 Backup saved: {backup_file} ({len(df_backup)} unique articles)\n")
        
        # Delay between requests (be respectful to the server)
        time.sleep(random.uniform(0.5, 1.5))
    
    # Save final results
    if all_articles:
        print("\n" + "=" * 70)
        print("SAVING RESULTS...")
        print("=" * 70)
        
        df = pd.DataFrame(all_articles)
        
        # Remove duplicates
        original_count = len(df)
        df = df.drop_duplicates(subset=['Article URL'])
        duplicates_removed = original_count - len(df)
        
        # Sort by date (newest first)
        df['Date_obj'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date_obj', ascending=False)
        
        # Keep the Date column for reference, remove helper column
        df = df[['Article URL', 'Article Title', 'Website Address', 'Date']]
        
        # Save to Excel
        df.to_excel(OUTPUT_FILE, index=False)
        
        print(f"✅ SUCCESS!")
        print(f"   Total articles scraped: {original_count}")
        print(f"   Unique articles: {len(df)}")
        print(f"   Duplicates removed: {duplicates_removed}")
        print(f"   Date range: {df['Date_obj'].min()} to {df['Date_obj'].max()}")
        print(f"   Output file: {OUTPUT_FILE}")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("⚠ NO ARTICLES FOUND")
        print("=" * 70)
        print("Possible reasons:")
        print("  1. No articles exist in the specified date range")
        print("  2. Website structure changed")
        print("  3. Network/connection issue")
        print("  4. Page range is incorrect")

if __name__ == "__main__":
    main()