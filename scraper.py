from collections import Counter
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from threading import Lock
from simhash import Simhash, SimhashIndex
from collections import defaultdict
import os
import json

visited_urls = set()
word_counts = {}
longest_page_url = ""
longest_page_word_count = 0
redirect_count = Counter()
page_hashes = {}
index = SimhashIndex([], k=3)
robots_cache = {}
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

def get_all_file_paths(folder_path):
    """
    Recursively get all file paths from the given folder.
    
    :param folder_path: Path to the folder containing JSON files
    :return: List of file paths
    """
    file_paths = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.json'):
                file_paths.append(os.path.join(root, file))
    return file_paths

def parse_file_and_tokenize(file_path):
    """
    Parse a JSON file, extract HTML content, and tokenize.
    
    :param file_path: Path to the JSON file containing HTML content
    :return: List of tokens extracted from the content
    """
    # Read the JSON file
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        
        # Extract the content field
        html_content = data.get("content", "")
        
        # Parse HTML and extract text
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.get_text().lower()
        
        # Tokenize the text content, removing stopwords and digits
        words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text_content)
                 if word not in stop_words and not word.isdigit()]
        
        return words

def build_inverted_index(file_paths):
    """
    Build an inverted index from a list of JSON files.
    
    :param file_paths: List of file paths to JSON files containing HTML content
    :return: Inverted index (dict), with tokens as keys and list of postings as values
    """
    inverted_index = defaultdict(list)
    
    for file_path in file_paths:
        tokens = parse_file_and_tokenize(file_path)
        doc_id = file_path  # Use file path as a document identifier
        #这边我感觉不太行。他Lecture讲了file path太长可能不适合做id。
        
        # Calculate term frequency (TF) for each token in this document
        term_freq = defaultdict(int)
        for token in tokens:
            term_freq[token] += 1
        
        # Add postings to the inverted index
        for token, tf in term_freq.items():
            inverted_index[token].append({'doc_id': doc_id, 'tf': tf})
    
    return inverted_index

def handle_response_error(resp):
    """Handle response errors based on status code"""
    if not resp or not hasattr(resp, 'error'):
        return False
        
    error_code = resp.error
    
    # Critical errors that must be handled
    if error_code == 603:  # Invalid scheme
        print(f"Error 603: URL scheme must be http or https")
        return False
    elif error_code == 604:  # Domain not in spec
        print(f"Error 604: Domain must be within specified domains")
        return False
    elif error_code == 605:  # Invalid file extension
        print(f"Error 605: Invalid file extension detected")
        return False
    elif error_code == 608:  # Robots.txt denial
        print(f"Error 608: Access denied by robots.txt")
        return False
        
    # Handle other errors we choose to process
    elif error_code == 607:  # Content too big
        print(f"Error 607: Content exceeds size limit - {resp.headers.get('content-length', 'unknown')} bytes")
        return False
    elif error_code == 606:  # URL parsing error
        print(f"Error 606: Cannot parse URL")
        return False
        
    # Ignorable errors
    elif error_code in [600, 601, 602]:  # Request malformed, Download exception, Server failure
        print(f"Ignorable error {error_code}: Continuing with next URL")
        return True
        
    return True

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
    """Main scraper function with error handling"""
    global longest_page_word_count, longest_page_url, visited_urls, word_counts, refresh_count
    
    # Initialize from existing data
    if len(visited_urls) == 0:
        visited_urls = unique_pages
        read_from_output()
    if len(word_counts) == 0:
        word_counts = w_counts
    if longest_page_url == "":
        longest_page_url = longest_url
    if longest_page_word_count == 0:
        longest_page_word_count = longest_count

    # Handle response errors
    if not handle_response_error(resp):
        return []

    # Check content size (Error 607)
    if resp.raw_response and 'content-length' in resp.raw_response.headers:
        content_length = int(resp.raw_response.headers['content-length'])
        if content_length > 10_000_000:  # 10MB limit example
            print(f"Error 607: Content too large ({content_length} bytes)")
            return []

    links = extract_next_links(url, resp)
    valid_links = []
    
    for link in links:
        try:
            if is_valid(link):
                valid_links.append(link)
        except Exception as e:
            print(f"Error 606: Exception in parsing URL {link}: {e}")
            continue

    visited_urls.update(valid_links)
    
    # Periodic output update
    if refresh_count >= 50:
        write_to_output()
        refresh_count = 0
    else:
        refresh_count += 1
    
    return valid_links

def extract_next_links(url, resp):
    """Extract links with error handling"""
    global longest_page_word_count, longest_page_url, word_counts
    
    links = []
    
    try:
        if resp.status == 200 and resp.raw_response and resp.raw_response.content:
            soup = BeautifulSoup(resp.raw_response.content, 'lxml')
            
            # Process text content
            text_content = soup.get_text().lower()
            words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text_content) 
                    if word not in stop_words and not word.isdigit()]
            
            # Duplicate detection
            current_simhash = Simhash(text_content)
            if index.get_near_dups(current_simhash):
                return []
            index.add(url, current_simhash)
            
            # Update statistics
            if len(words) > longest_page_word_count:
                longest_page_word_count = len(words)
                longest_page_url = url
            
            for word in words:
                word_counts[word] = word_counts.get(word, 0) + 1
            
            # Extract links
            for anchor in soup.find_all('a', href=True):
                try:
                    abs_url, _ = urldefrag(urljoin(url, anchor['href']))
                    links.append(abs_url)
                except Exception as e:
                    print(f"Error 606: Failed to parse link {anchor['href']}: {e}")
                    continue
                    
    except Exception as e:
        print(f"Error 601: Exception in processing page {url}: {e}")
        return []
        
    return links


def is_valid(url):
    """Validate URL with comprehensive error handling"""
    try:
        parsed = urlparse(url)
        
        # Error 603: Check scheme
        if parsed.scheme not in set(["http", "https"]):
            print(f"Error 603: Invalid scheme in {url}")
            return False
            
        # Error 604: Check domain
        netloc_parts = parsed.netloc.split('.')
        if len(netloc_parts) < 2:
            print(f"Error 604: Invalid domain format in {url}")
            return False
            
        domain = ".".join(netloc_parts[-2:])
        allowed_subdomains = ["ics", "cs", "informatics", "stat"]
        
        # Special case for today.uci.edu
        if parsed.netloc == "today.uci.edu":
            if not parsed.path.startswith("/department/information_computer_sciences/"):
                print(f"Error 604: Invalid today.uci.edu path in {url}")
                return False
            return True
            
        # Domain validation
        if domain != "uci.edu":
            print(f"Error 604: Domain not allowed: {domain}")
            return False
        if len(netloc_parts) > 2 and netloc_parts[-3] not in allowed_subdomains:
            print(f"Error 604: Subdomain not allowed: {netloc_parts[-3]}")
            return False
            
        # Error 605: Check file extension
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            print(f"Error 605: Invalid file extension in {url}")
            return False
            
        # Additional validations
        if is_repeating_path(parsed.path):
            return False
        
        if len(parsed.path.split("/")) > 5:
            return False
            
        if re.search(r'\d{4}-\d{2}', url):
            return False
            
        if url in visited_urls:
            return False
            
        if parsed.query.count("%") >= 3 or parsed.query.count("=") >= 3 or parsed.query.count("&") >= 3:
            return False
            
        # Trap detection
        php_url = url.strip().split(".php")[0] + ".php"
        if php_blacklist[php_url] > 10:
            return False
        php_blacklist[php_url] += 1
        
        if count_blacklist[parsed.netloc + parsed.path] > 10:
            return False
        count_blacklist[parsed.netloc + parsed.path] += 1
        
        return True
        
    except Exception as e:
        print(f"Error 606: Exception in parsing URL {url}: {e}")
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
