
import os
import pickle
import queue
import sys
import threading
import time
# import XMLExtract
import urllib

import pandas as pd
import requests
import wget

import EdgarScrapeMin
# import logs
import settings
from datetime import datetime
import xml.etree.cElementTree as ET
from bs4 import BeautifulSoup as BS

from google.cloud import datastore

# import sys
# sys.path.append('/Applications/PyVmMonitor.app/Contents/MacOS/public_api')
# import pyvmmonitor
# @pyvmmonitor.profile_method


def create_client(project_id):
    return datastore.Client(project_id)



class ScrapeAndExtractThreaded:

    def __init__(self, symbol_queue, xml_queue, download_queue):
        self.symbol_queue = symbol_queue
        self.xmlqueue = xml_queue
        self.download_queue = download_queue

        #Scrape Section
        self.stock_lists = os.listdir(settings.STOCK_EXCHANGE_LIST_PATH)
        self.symbol_keys = []
        self.scraped_keys = None
        self.finished = False

        #Extract Section
        self.extracted_keys = None
        self.to_extract = queue.Queue()

        self.populate_symbol_keys()
        # self.get_all_keys()
        self.scraped_keys = []
        self.extracted_keys = []
        self.client = self.create_client(settings.GCLOUD_PROJECT_ID)

    def create_client(self, project_id):
        return datastore.Client(project_id)

    def add_company(self, symbol, rss_url, rss_valid):
        key = self.client.key('Company', symbol)
        company = datastore.Entity(key)
        company.update({
            'symbol': symbol,
            'rss_url': rss_url,
            'rss_valid': rss_valid,
            'date_created': datetime.utcnow()
        })
        self.client.put(company)
        return company.key

    def add_filing(self, return_filing, symbol, date, index_href):
        parent_key = self.client.key('Company', symbol)
        key = self.client.key('Filing', date, parent=parent_key)
        filing = datastore.Entity(key)
        filing.update({
            'symbol': symbol,
            'date': date,
            'index_href': index_href,
            'index_scraped': False,
            'date_created': datetime.utcnow()
        })
        if not return_filing:
            self.client.put(filing)
            return filing.key
        else:
            return filing

    def add_batch_filings(self, filings):
        filing_batch = [
            self.add_filing(return_filing=True, **filing) for filing in filings
            ]
        self.client.put_multi(filing_batch)
        return filing_batch

    def add_download(self, symbol, date, download_href, filepath, filename):
        parent_key = self.client.key('Company', symbol, 'Filing', date)
        key = self.client.key('Download', parent=parent_key)
        download = datastore.Entity(key)
        download.update({
            'symbol': symbol,
            'date': date,
            'download_href': download_href,
            'filepath': filepath,
            'filename': filename,
            'dir_created': False,
            'downloaded': False,
            'date_created': datetime.utcnow()
        })
        self.client.put(download)
        return download.key

    def mark_downloaded(self, download_key):
        with self.client.transaction():
            # key = self.client.key(download_key)
            download = self.client.get(download_key)

            if not download:
                raise ValueError(
                    'Download {} does not exist.'.format(download_key))

            download['downloaded'] = True
            self.client.put(download)

    def populate_symbol_keys(self):
        '''Grab all the keys from file'''
        for xlist in self.stock_lists:
            f = pd.read_csv('{0}/{1}'.format(settings.STOCK_EXCHANGE_LIST_PATH, xlist))
            for symbol in f[f.columns[0]]:
                if symbol not in self.symbol_keys:
                    self.symbol_keys.append(symbol)

    def get_all_keys(self):
        scraped_data_log = pickle.load(open(settings.SCRAPE_LOG_FILE_PATH, "rb"))
        self.scraped_keys = list(scraped_data_log.keys())
        extracted_data_log = pickle.load(open(settings.EXTRACT_LOG_FILE_PATH, "rb"))
        self.extracted_keys = list(extracted_data_log.keys())
        sk_set = set(self.scraped_keys)
        ek_set = set(self.extracted_keys)
        to_extract = list(sk_set - ek_set)
        for te in to_extract:
            self.to_extract.put(te)

    @staticmethod
    def scrape_symbol(symbol, download_queue):
        sym_gf = EdgarScrapeMin.GetFilings(symbol, download_queue)
        # if sym_gf.filings['errors']['count'] > 0:
        #     soe_error_data = sym_gf.filings['errors']
        #     logs.add_scrape_data(symbol, soe_error_data, False)
        # if sym_gf.filings['success']['count'] > 0:
        #     soe_success_data = sym_gf.filings['success']
        #     logs.add_scrape_data(symbol, soe_success_data, True)
        # else:
        #     logs.add_scrape_data(symbol, None, True)

    
    def scrape_xml_index(self, symbol, filing_type='10-k'):
        link = settings.RSS_XML_URL.format(symbol, filing_type)
        r = requests.get(link)
        # company = {'symbol': symbol, 'filings': []}
        try:
            root = ET.fromstring(r.content)
            ns = {'role': 'http://www.w3.org/2005/Atom'}
            company_entity = self.add_company(symbol, link, rss_valid=True)
            # filings = []
            for entry in root.findall('role:entry/./role:content', ns):
                filing = {
                    'symbol': symbol,
                    'date': entry.find('role:filing-date', ns).text,
                    'index_href': entry.find('role:filing-href', ns).text
                }
                # filings.append(filing)
                filing_entity = self.add_filing(return_filing=False, **filing)
                self.xmlqueue.put(filing)
            # filing_batch_entities = self.add_batch_filings(filings)
        except ET.ParseError:
            company_entity = self.add_company(symbol, link, rss_valid=False)
            # print("Invalid Symbol: {}\t{}".format(symbol, link))
        # self.xmlqueue.put(company)

    def get_download_link(self, filing):
        filing_download = filing
        r = requests.get(filing_download['index_href'])
        s = BS(r.text, "lxml")
        try:
            html_link = s.find_all('table', {
                'class': 'tableFile',
                'summary': 'Document Format Files'
                })[0].find_all('tr')[1].find('a')['href']
            xtname = ['.html', '.htm']
            if os.path.splitext(html_link)[1] in xtname:
                html_link = 'https://www.sec.gov' + html_link
                filing_download['download_href'] = html_link
                self.queue_download(filing_download)
            else:
                return False
        except IndexError:
            return False

    def check_duplicate(self, directory, filename):
        if filename in os.listdir(directory):
            return True
        return False

    def queue_download(self, filing):
        fname = "{0}_{1}_{2}.html".format(
            filing['symbol'], filing['date'], 'html')
        diry = '{0}/{1}/html/10-K/{2}/'.format(
            settings.RAW_DATA_PATH, filing['symbol'], filing['date'])
        if not os.path.exists(diry):
            try:
                os.makedirs(diry)
            except:
                print("Unexpected error: {}".format(sys.exc_info()[0]))

        if not self.check_duplicate(diry, fname):
            filing.update({'filepath': diry, 'filename': fname})
            download_entity = self.add_download(
                symbol=filing['symbol'],
                date=filing['date'],
                download_href=filing['download_href'],
                filepath=filing['filepath'],
                filename=filing['filename']
            )
            download_request = (filing['download_href'], '{0}{1}'.format(diry, fname), download_entity)
            self.download_queue.put(download_request)

    def queue_scrape_list(self):
        """Queues up all symbols in list."""
        for symbol in self.symbol_keys:
            if symbol in self.scraped_keys:
                continue
            # self.scrape_symbol(symbol)
            self.symbol_queue.put(symbol)
            print("Putting symbol {}".format(symbol))
            # self.scraped_keys.append(symbol)
            # self.to_extract.put(symbol)
        self.finished = True


# scrape_and_extract = ScrapeAndExtractThreaded()


def run_main_threaded():
    '''Runs the threaded downloader'''
    symbolqueue = queue.Queue()
    xmlqueue = queue.Queue()
    downloadqueue = queue.Queue()
    # logqueue = queue.Queue()
    sc = ScrapeAndExtractThreaded(symbolqueue, xmlqueue, downloadqueue)

    class XMLThread(threading.Thread):
        '''Gathers links from xml urls
        Puts {symbol, date, filingurl} on filing scrape queue'''
        def __init__(self, name, symbol_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.symbol_queue = symbol_queue
            # self.xml_queue = xml_queue

        def run(self):
            while True:
                symbol = self.symbol_queue.get()
                sc.scrape_xml_index(symbol)
                # logqueue.put("Symbol")
                self.symbol_queue.task_done()


    class XMLPrintThread(threading.Thread):
        '''Gathers links from xml urls
        Puts {symbol, date, filingurl} on filing scrape queue'''
        def __init__(self, name, xml_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.xml_queue = xml_queue

        def run(self):
            while True:
                company = self.xml_queue.get()
                print("{}\t{} Left in XML Queue".format(company, self.xml_queue.qsize()))
                self.xml_queue.task_done()


    class FilingsThread(threading.Thread):
        '''Symbol-Scrape EdgarFilings Thread
        Scrapes sec.gov/edgar for 10-k html filings for specific ticker symbol'''
        def __init__(self, name, symbol_queue, download_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.symbol_queue = symbol_queue
            self.download_queue = download_queue

        def run(self):
            while True:
                # Grabs symbol from queue
                symbol = self.symbol_queue.get()
                print("{} Left in Scrape Queue".format(self.symbol_queue.qsize()))
                # Runs the symbol scraper to generate download urls and filepath
                sc.scrape_symbol(symbol, self.download_queue)
                self.symbol_queue.task_done()

    class IndexScrapeThread(threading.Thread):
        '''Index-Scrape EdgarFilings Thread
        Scrapes sec.gov/edgar for 10-k html filings for specific ticker symbol'''
        def __init__(self, name, xml_queue, download_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.xml_queue = xml_queue
            # self.download_queue = download_queue

        def run(self):
            while True:
                # Grabs symbol from queue
                filing = self.xml_queue.get()
                # print("\t{} Index Queue".format(self.xml_queue.qsize()))
                # Runs the symbol scraper to generate download urls and filepath
                sc.get_download_link(filing)
                self.xml_queue.task_done()

    class DownloadThread(threading.Thread):
        '''HTML 10-K Downloader Thread'''
        def __init__(self, name, symbol_queue, xml_queue, download_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.symbol_queue = symbol_queue
            self.xml_queue = xml_queue
            self.download_queue = download_queue

        def run(self):
            while True:
                download_info = self.download_queue.get()
                # print(download_info)
                download_url, download_path, download_entity = download_info
                wget.download(download_url, download_path)
                # sc.mark_downloaded(download_entity)
                print("\t\t{} Symbol\t{} XML\t{} Download".format(
                    self.symbol_queue.qsize(),
                    self.xml_queue.qsize(),
                    self.download_queue.qsize()))
                self.download_queue.task_done()

    class LogThread(threading.Thread):
        '''Prints out all the good logs'''
        def __init__(self, name, symbol_queue, xml_queue, download_queue, log_queue):
            threading.Thread.__init__(self)
            self.name = name
            self.symbol_queue = symbol_queue
            self.xml_queue = xml_queue
            self.download_queue = download_queue
            self.log_queue = log_queue

        def run(self):
            while True:
                # toprint = self.log_queue.get()
                print("[{}]: \t{} Symbol\t{} XML\t{} Download\tLOG:".format(
                    datetime.utcnow(),
                    self.symbol_queue.qsize(),
                    self.xml_queue.qsize(),
                    self.download_queue.qsize()
                    # toprint
                    ))
                # self.log_queue.task_done()

    # log_thread = LogThread(
    #     "Log_Thread",
    #     symbolqueue,
    #     xmlqueue,
    #     downloadqueue,
    #     logqueue
    #     )
    # log_thread.setDaemon(True)
    # log_thread.start()


    print("Main Thread - Starting XML Scrape Thread Pool")
    for i in range(5):
        xml_thread_name = "XMLThread-[{}]".format(i+1)
        xt = XMLThread(
            xml_thread_name,
            symbolqueue
            )
        xt.setDaemon(True)
        xt.start()
        print("Starting XML thread #{}".format(i+1))

    print("Main Thread - Starting Index Scrape Thread Pool")
    for i in range(5):
        ix_thread_name = "IndexScrapeThread-[{}]".format(i+1)
        ix = IndexScrapeThread(
            ix_thread_name,
            xmlqueue,
            downloadqueue
            )
        ix.setDaemon(True)
        ix.start()
        print("Starting IndexScrape thread #{}".format(i+1))

    print("Main Thread - Starting Download Thread Pool")
    for i in range(5):
        download_thread_name = "DownloadThread-[{}]".format(i+1)
        dt = DownloadThread(
            download_thread_name,
            symbolqueue,
            xmlqueue,
            downloadqueue
            )
        dt.setDaemon(True)
        print("Starting Download thread #{}".format(i+1))
        dt.start()

    sc.queue_scrape_list()

    # symbolqueue.join()


    timestart = datetime.now()
    print("The game has begun. The time is {}".format(timestart))
    symbolqueue.join()

    time_symbolqueue_close = datetime.now()

    print("XML Thread Pool closed: The time is {}".format(time_symbolqueue_close))

    print("Main Thread - ADDING MORE Index Scrape Thread Pool")
    for i in range(5, 10):
        ix_thread_name = "IndexScrapeThread-[{}]".format(i+1)
        ix = IndexScrapeThread(ix_thread_name, xmlqueue, downloadqueue)
        ix.setDaemon(True)
        ix.start()
        print("Starting IndexScrape thread #{}".format(i+1))

    xmlqueue.join()
    time_indexscrapequeue_close = datetime.now()
    print("Index Scrape Thread Pool closed: The time is {}".format(time_indexscrapequeue_close))

    print("Main Thread - ADDING MORE Download Thread Pool")
    for i in range(5, 10):
        download_thread_name = "DownloadThread-[{}]".format(i+1)
        dt = DownloadThread(download_thread_name, symbolqueue, xmlqueue, downloadqueue)
        dt.setDaemon(True)
        print("Starting Download thread #{}".format(i+1))
        dt.start()

    downloadqueue.join()
    # logqueue.join()
    time_downloadqueue_close = datetime.now()
    print("The download queue has completed successfully")
    print("The time is {}".format(time_downloadqueue_close))
    print("Time elapsed is {}".format(time_downloadqueue_close - timestart))
    print("Happy pipelining you filthy worms")

if __name__ == '__main__':
    run_main_threaded()
