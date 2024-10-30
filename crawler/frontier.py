import os
import shelve
import time
from urllib.parse import urlparse
from threading import RLock
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class DomainAccessManager:
    def __init__(self, politeness_delay):
        self.domain_locks = {}
        self.domain_last_access = {}
        self.lock = RLock()
        self.politeness_delay = politeness_delay
    
    def wait_and_update(self, domain):
        with self.lock:
            if domain not in self.domain_locks:
                self.domain_locks[domain] = RLock()
                
        with self.domain_locks[domain]:
            current_time = time.time()
            if domain in self.domain_last_access:
                elapsed_time = current_time - self.domain_last_access[domain]
                if elapsed_time < self.politeness_delay:
                    time.sleep(self.politeness_delay - elapsed_time)
            
            self.domain_last_access[domain] = time.time()

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.lock = RLock()
        self.domain_manager = DomainAccessManager(politeness_delay=0.5)
        
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self.lock:
            try:
                url = self.to_be_downloaded.pop()
                domain = self.get_domain(url)
                # 在返回URL之前确保遵守礼貌延迟
                self.domain_manager.wait_and_update(domain)
                return url
            except IndexError:
                return None

    def add_url(self, url):
        url = normalize(url)
        with self.lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)

    def mark_url_complete(self, url):
        with self.lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.save[urlhash] = (url, True)
            self.save.sync()

    @staticmethod
    def get_domain(url):
        return urlparse(url).netloc
