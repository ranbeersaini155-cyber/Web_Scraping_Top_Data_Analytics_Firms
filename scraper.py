#!/usr/bin/env python3
"""
scraper.py
A small script to scrape company name, company profile URL, company geo (location) and company email addresses
from GoodFirms data-analytics listing pages.

Usage:
    python scraper.py --pages 3 --out companies.csv

Notes:
- The script fetches the listing pages and then visits each company profile to search for email addresses (mailto links
  and plain-text emails).
- Be respectful: set reasonable --pages and add delays to avoid overloading the site.
- Requires: requests, beautifulsoup4, pandas
"""

import re
import time
import argparse
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

USER_AGENT = "Mozilla/5.0 (compatible; WebScraper/1.0; +https://github.com/ranbeersaini155-cyber/Web_Scraping_Top_Data_Analytics_Firms)"
HEADERS = {"User-Agent": USER_AGENT}
BASE_LISTING = "https://www.goodfirms.co/big-data-analytics/data-analytics"

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def fetch(url, retry=2, timeout=15):
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retry:
                time.sleep(2 + attempt)
                continue
            raise

def parse_listing_page(html):
    """Return list of company entries each as dict with name, profile_href, listing_location"""
    bs = BeautifulSoup(html, "lxml")
    name_spans = bs.find_all('span', {'itemprop': 'name'})
    loc_divs = bs.find_all('div', {'class': 'firm-location'})

    entries = []
    for idx, span in enumerate(name_spans):
        name = span.get_text(strip=True)
        profile_href = None
        parent_a = span.find_parent('a')
        if parent_a and parent_a.has_attr('href'):
            profile_href = parent_a['href']
        location = None
        if idx < len(loc_divs):
            location = loc_divs[idx].get_text(separator=", ", strip=True)
        entries.append({
            'name': name,
            'profile_href': profile_href,
            'listing_location': location,
        })
    return entries

def extract_emails_from_profile(profile_url):
    """Fetch profile page and extract emails (mailto or plain text). Returns list of unique emails."""
    try:
        resp = fetch(profile_url)
    except Exception:
        return []
    html = resp.text
    emails = set()
    bs = BeautifulSoup(html, 'lxml')
    for a in bs.select('a[href^=mailto]'):
        href = a.get('href')
        if href:
            m = EMAIL_RE.search(href)
            if m:
                emails.add(m.group(0))
    for m in EMAIL_RE.findall(html):
        emails.add(m)
    cleaned = sorted(emails)
    return cleaned

def resolve_profile_url(href):
    if not href:
        return None
    if href.startswith('http'):
        return href
    return urljoin(BASE_LISTING, href)

def scrape(pages=3, delay=1.5):
    all_companies = []
    for page in range(1, pages + 1):
        url = BASE_LISTING
        if page > 1:
            url = f"{BASE_LISTING}?page={page}"
        print(f"Fetching listing page: {url}")
        resp = fetch(url)
        entries = parse_listing_page(resp.text)
        print(f"  found {len(entries)} company name entries on page {page}")

        for e in entries:
            name = e['name']
            profile_href = e.get('profile_href')
            profile_url = resolve_profile_url(profile_href) if profile_href else None
            listing_location = e.get('listing_location')
            emails = []
            if profile_url:
                try:
                    print(f"    Visiting profile: {profile_url}")
                    emails = extract_emails_from_profile(profile_url)
                    time.sleep(delay)
                except Exception as ex:
                    print(f"      Failed to fetch profile {profile_url}: {ex}")
            company = {
                'company_name': name,
                'company_profile_url': profile_url,
                'company_geo': listing_location,
                'company_emails': "; ".join(emails) if emails else '',
            }
            all_companies.append(company)
        time.sleep(delay)

    df = pd.DataFrame(all_companies)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape GoodFirms data analytics listing for company info')
    parser.add_argument('--pages', type=int, default=3, help='Number of listing pages to fetch (default 3)')
    parser.add_argument('--delay', type=float, default=1.5, help='Delay between requests in seconds')
    parser.add_argument('--out', type=str, default='companies.csv', help='Output CSV filename')
    args = parser.parse_args()

    print('Starting scraper...')
    df = scrape(pages=args.pages, delay=args.delay)
    print(f'Writing {len(df)} rows to {args.out}')
    df.to_csv(args.out, index=False)
    print('Done')