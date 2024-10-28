import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import Counter

# Global variables for tracking page and word statistics
visited_pages = set()
subdomain_count = Counter()
word_count = Counter()
longest_page = {'url': '', 'word_count': 0}

def scraper(url, resp):
    """
    Main scraper function that returns valid links from a webpage.
    Args:
        url: URL of the page
        resp: Response object containing page content
    Returns:
        list: Valid URLs extracted from the page
    """
    links = extract_next_links(url, resp)
    # print_statistics()
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """
    Extracts and processes links from a webpage.
    Args:
        url: URL of the page
        resp: Response object containing page content
    Returns:
        list: URLs extracted from the page
    """
    global longest_page
    # Check if response status is valid
    if resp.status != 200 or not resp.raw_response or not hasattr(resp.raw_response, 'content'):
        return []
        
    links = []

    try:
        # Use BeautifulSoup to parse the page content
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser', from_encoding='utf-8')
        
        # Process page content and count words
        text = soup.get_text()
        words = re.findall(r"\w+", text.lower())
        current_word_count = len(words)
        word_count.update(word for word in words if word not in stop_words())

        # Update longest page information
        if current_word_count > longest_page['word_count']:
            longest_page = {'url': url, 'word_count': current_word_count}

        # Record visited page
        visited_pages.add(remove_fragment(url))

        # Track subdomain information
        parsed_url = urlparse(url)
        if parsed_url.netloc.endswith(".uci.edu"):
            subdomain_count[parsed_url.netloc] += 1
        
        # Extract all <a> tags with href attributes
        for link in soup.find_all('a', href=True):
            href = link.get('href').strip()
            if not href:
                continue
                
            # Convert relative URLs to absolute URLs
            parsed_href = urlparse(href)
            if not parsed_href.scheme:
                parsed_base = urlparse(url)
                href = parsed_base.scheme + '://' + parsed_base.netloc + '/' + href.lstrip('/')
            
            # Add defragmented URL to links
            processed_url = remove_fragment(href)
            if processed_url:
                links.append(processed_url)

    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return []

    return links

def is_valid(url):
    """
    Validates whether the URL should be crawled.
    Args:
        url: URL to validate
    Returns:
        bool: True if URL is valid, False otherwise
    """
    try:
        parsed = urlparse(url)
        
        # Check URL scheme
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Check file extensions
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
            
        # Validate domains
        allowed_domains = {
            ".ics.uci.edu",
            ".cs.uci.edu",
            ".informatics.uci.edu",
            ".stat.uci.edu"
        }

        # Special case for today.uci.edu
        if parsed.netloc == "today.uci.edu":
            if "/department/information_computer_sciences/" not in parsed.path:
                return False
        elif not any(domain in parsed.netloc for domain in allowed_domains):
            return False

        # Avoid pages with too many query parameters
        if len(parsed.query) > 100:
            return False

        # Avoid calendar and event trap pages with query parameters
        if any(trap in parsed.path.lower() for trap in ["/calendar/", "/events/"]):
            if len(parsed.query) > 0:
                return False

        return True

    except TypeError as e:
        print(f"TypeError for {parsed}: {str(e)}")
        raise

def remove_fragment(url):
    """
    Removes the fragment part of a URL.
    Args:
        url: URL to process
    Returns:
        str: URL without fragment
    """
    try:
        parsed = urlparse(url)
        return parsed._replace(fragment="").geturl()
    except Exception as e:
        print(f"Error removing fragment from {url}: {str(e)}")
        return ""

def stop_words():
    """Returns a set of common English stop words."""
    return {
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", 
        "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", 
        "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", 
        "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", 
        "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", 
        "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", 
        "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", 
        "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", 
        "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", 
        "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", 
        "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", 
        "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", 
        "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", 
        "they're", "they've", "this", "those", "through", "to", "too", "under", "until", 
        "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", 
        "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", 
        "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", 
        "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
    }

def print_statistics():
    """Prints crawling statistics."""
    global visited_pages, subdomain_count, word_count, longest_page

    print("\n=== Crawling Statistics ===")
    print(f"\nNumber of unique pages found: {len(visited_pages)}")
    print(f"\nLongest page: {longest_page['url']} with {longest_page['word_count']} words")
    
    print("\nTop 50 most common words:")
    for word, count in word_count.most_common(50):
        print(f"{word}: {count}")
    
    print("\nSubdomains found:")
    for subdomain, count in sorted(subdomain_count.items()):
        print(f"{subdomain}, {count}")
