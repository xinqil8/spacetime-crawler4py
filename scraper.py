from collections import Counter
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from threading import Lock
import simhash

# Global variables
visited_urls = set()
word_counts = {}
longest_page_url = ""
longest_page_word_count = 0
redirect_count = Counter()
# page_hashes = {}
# index = simhash.SimhashIndex([], k=3)

# Add lock for thread-safe file operations
output_lock = Lock()

refresh_count = 0
php_blacklist = Counter()
count_blacklist = Counter()

# Default English stopwords list
stop_words = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", 
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", 
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", 
    "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", 
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", 
    "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", 
    "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", 
    "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", 
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", 
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", 
    "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", 
    "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", 
    "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", 
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", 
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", 
    "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
])

def write_to_output():
    """Thread-safe function to write stats to output.txt"""
    with output_lock:
        try:
            with open("output.txt", 'w') as f:
                # Write unique pages count
                f.write(f"Unique pages: {len(visited_urls)}\n")
                
                # Write longest page information
                f.write(f"Longest page so far: {longest_page_url} with word count: {longest_page_word_count}\n")
                
                # Write top 50 words
                sorted_words = dict(sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:50])
                f.write(f"Top 50 words: {sorted_words}\n")
                
                # Write subdomain statistics
                subdomains = Counter()
                for url in visited_urls:
                    parsed_url = urlparse(url)
                    if parsed_url.netloc.endswith('.ics.uci.edu'):
                        subdomains[parsed_url.netloc] += 1
                
                for subdomain, count in sorted(subdomains.items()):
                    f.write(f"{subdomain}, {count}\n")
        except Exception as e:
            print(f"Error writing to output.txt: {e}")

def read_from_output():
    """Read and restore stats from output.txt if it exists"""
    global word_counts, longest_page_url, longest_page_word_count
    
    try:
        with open('output.txt', 'r') as f:
            lines = f.readlines()
            for line in reversed(lines):
                # Restore word counts
                if line.startswith("Top 50 words:"):
                    dict_str = line.replace("Top 50 words: ", "").strip()
                    word_counts = eval(dict_str)  # Using eval since we know the format is safe
                
                # Restore longest page information
                elif line.startswith("Longest page so far:"):
                    parts = line.strip().split(' ')
                    longest_page_url = parts[4]
                    longest_page_word_count = int(parts[-1])
                    break
    except FileNotFoundError:
        # Initialize with empty values if file doesn't exist
        word_counts = {}
        longest_page_word_count = 0
        longest_page_url = ""
    except Exception as e:
        print(f"Error reading from output.txt: {e}")
        # Initialize with empty values on error
        word_counts = {}
        longest_page_word_count = 0
        longest_page_url = ""




def scraper(url, resp, unique_pages, w_counts, longest_url, longest_count):
    global longest_page_word_count, longest_page_url, visited_urls, word_counts
    
    # Initialize from existing data or output.txt
    if len(visited_urls) == 0:
        visited_urls = unique_pages
        read_from_output()  # Try to restore data from output.txt
    if len(word_counts) == 0:
        word_counts = w_counts
    if longest_page_url == "":
        longest_page_url = longest_url
    if longest_page_word_count == 0:
        longest_page_word_count = longest_count

    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]
    visited_urls.update(valid_links)
    
    # Write statistics to output.txt periodically
    global refresh_count
    if refresh_count >= 50:
        write_to_output()
        refresh_count = 0
    else:
        refresh_count += 1
    
    return valid_links


def extract_next_links(url, resp):
    global longest_page_word_count, longest_page_url, visited_urls, word_counts, refresh_count
    
    links = []

    

    if resp.status == 200:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')

        text_content = soup.get_text().lower()
        words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text_content) 
                if word not in stop_words and not word.isdigit()]
        
        # #check current hash
        # current_simhash = simhash(text_content)
        
        # # use hash to check similiar page
        # if index.get_near_dups(current_simhash):
        #     print(f"Skipping similar page: {url}")
        #     return []

        # # update simhash
        # index.add(url, current_simhash)


        #Update longest page statistics
        if len(words) > longest_page_word_count:
            longest_page_word_count = len(words)
            longest_page_url = url

        # Update word counts
        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1

        # Extract links
        for anchor in soup.find_all('a', href=True):
            abs_url, _ = urldefrag(urljoin(url, anchor['href']))
            links.append(abs_url)
    

    return links

def is_valid(url):
    global longest_page_word_count, longest_page_url, visited_urls, php_blacklist, redirect_count

    try:
        parsed = urlparse(url)
        # Only allow certain subdomains from 'uci.edu'
        allowed_subdomains = ["ics", "cs", "informatics", "stat"]
        allowed_domains = ["uci.edu"]

        # Split the netloc into parts
        netloc_parts = parsed.netloc.split('.')

        # Ensure the netloc has at least two parts for domain and TLD
        if len(netloc_parts) < 2:
            return False
        
        
        #check if it redirects too many times(more than 5), then its a trap)
        if redirect_count[url] > 5:
            return False
        else:
            redirect_count[url] += 1


        # Create a domain string from the last two parts of netloc
        domain = ".".join(netloc_parts[-2:])

        # Check if the domain is in the allowed domains list
        if domain in allowed_domains:
            # Check if the subdomain is allowed if it exists
            if len(netloc_parts) > 2 and netloc_parts[-3] not in allowed_subdomains:
                return False
        else:
            return False

        # Check for repeating directory patterns
        if is_repeating_path(parsed.path):
            return False          

        # path is too long, possibly a trap
        if len(parsed.path.split("/")) > 5:
            return False
        
        # date is possibly a trap
        date_pattern = r'\d{4}-\d{2}'
        if re.search(date_pattern, url):
            return False
        
        if url in visited_urls:
            return False
        
        if parsed.query.count("%") >= 3 or parsed.query.count("=") >= 3 or parsed.query.count("&") >= 3:
            return False
        
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

        # if a url with .php, it's possible to be a trap
        php_url = url.strip().split(".php")[0] + ".php"
        if php_blacklist[php_url] > 10:
            return False
        else:
            php_blacklist[php_url] += 1

        # if a url's path appears too much time, it's possible to be a trap
        if count_blacklist[parsed.netloc + parsed.path] > 10:
            return False
        else:
            count_blacklist[parsed.netloc + parsed.path] += 1

        return True
    except Exception as e:
        print(f"An exception occurred for {url}: {e}")
        return False

def is_repeating_path(path):
    segments = path.strip("/").split('/')
    # Check for a repeating pattern where a segment is followed by itself
    for i in range(len(segments) - 1):
        if segments[i] == segments[i + 1]:
            return True
    # Use a dictionary to count occurrences of each segment
    segment_counts = {}
    for segment in segments:
        if segment not in segment_counts:
            segment_counts[segment] = 1
        else:
            segment_counts[segment] += 1
            # If a segment occurs more than 3 times, it's likely a trap
            if segment_counts[segment] >= 3:
                return True
    return False

def print_statistics():
    """Print current statistics and write to output file"""
    print("\nCrawler Statistics:")
    print(f"Total unique pages visited: {len(visited_urls)}")
    print(f"URL of the longest page: {longest_page_url}")
    print(f"Word count of the longest page: {longest_page_word_count}")
    print("Top 10 most frequent words:")
    for word, count in list(word_counts.items())[:10]:
        print(f"{word}: {count}")
    
    # Write current stats to output.txt
    write_to_output()
