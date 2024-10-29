from threading import Event, Thread, Timer
import signal
import sys
import time

from inspect import getsource
from urllib.parse import urldefrag
from utils.download import download
from utils import get_logger
import scraper

# Define the signal handler
def handle_interrupt(signum, frame):
    scraper.print_statistics()  # Use scraper's statistics function
    print("Process paused.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        unique_pages = set()
        word_counts = {}
        longest_page_url = ""
        longest_page_word_count = 0
        # Create an Event object for timeout handling
        timeout_event = Event()
        # Read the log file
        with open('Logs/Worker.log', 'r') as file:
            for line in file:
                # Extract the URL and the status
                if 'Downloaded' in line and 'status' in line:
                    url = line.split()[1].rstrip(',')
                    # Remove fragment and query to get the unique URL
                    url = urldefrag(url)[0]
                    unique_pages.add(url)
        
        try:
            while True:
                # Before each URL fetch, reset the timeout_event and start a Timer
                timeout_event.clear()
                timeout_timer = Timer(10, timeout_event.set)
                timeout_timer.start()

                try:
                    tbd_url = self.frontier.get_tbd_url()  # This method already includes the 500ms delay logic
                    if not tbd_url:
                        self.logger.info("Frontier is empty. Stopping Crawler.")
                        break
                    resp = download(tbd_url, self.config, self.logger)
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{resp.status}>, "
                        f"using cache {self.config.cache_server}.")
                    if resp.status == 200:
                        scraped_urls = scraper.scraper(tbd_url, resp, unique_pages, word_counts, longest_page_url, longest_page_word_count)
                        for scraped_url in scraped_urls:
                            self.frontier.add_url(scraped_url)
                        self.frontier.mark_url_complete(tbd_url)
                except Exception as e:
                    self.logger.error(f"An exception occurred: {e}")  # Log the actual exception
                    continue  # Continue with the next iteration of the loop
                finally:
                    timeout_timer.cancel()    

                # Check if the timeout was reached
                if timeout_event.is_set():
                    self.logger.info(f"Timeout reached for URL {tbd_url}. Skipping.")
                    continue  # Skip this URL and continue with the next one
                time.sleep(self.config.time_delay)      
            scraper.print_statistics()  # Print final statistics when done
        except Exception as e:
            scraper.print_statistics()  # Print statistics even if exception occurs
