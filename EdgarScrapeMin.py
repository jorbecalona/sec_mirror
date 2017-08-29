import os
import re
import urllib

import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm

import LinkURL
import settings
import lxml


class GetFilings:

    def __init__(self, ticker_symbol, download_queue):
        self.download_queue = download_queue
        self.ticker_symbol = ticker_symbol
        self.filings = {
            '10q_list': [],
            '10q_xl_list': [],
            '10q_xml': [],
            '10q_html': [],
            '10q_txt': [],
            '10q_xl': [],
            '10k_list': [],
            '10k_xl_list': [],
            '10k_xml': [],
            '10k_html': [],
            '10k_txt': [],
            '10k_xl': [],
            'success': {
                'count': 0,
                '10-Q': None,
                '10-K': None
            },
            'errors': {
                'count': 0,
                '10-Q': None,
                '10-K': None
            }
        }
        print('Scraping {0}'.format(self.ticker_symbol))
        # print('Getting 10-K list...')
        # self.get_10k_list()
        # print('Getting 10-K files...')
        self.get_all_10k()
        # print('Generating downloads for all files...')
        self.download_all()

    def create_folders(self):
        if not os.path.exists('{0}/{1}/'.format(
                settings.RAW_DATA_PATH, self.ticker_symbol)):
            os.makedirs(
                '{0}/{1}/'.format(settings.RAW_DATA_PATH, self.ticker_symbol))
        if settings.GET_HTML:
            if not os.path.exists('{0}/{1}/html/10-K/'.format(
                    settings.RAW_DATA_PATH, self.ticker_symbol)):
                os.makedirs(
                    '{0}/{1}/html/10-K/'.format(settings.RAW_DATA_PATH, self.ticker_symbol))

    def validate_page(self, html):
        try:
            check = html.find_all('center')[0].text
        except IndexError:
            self.create_folders()
            return True
        return False

    def get_main_html(self, q_or_k):
        '''Retrieves the URL for the SEC Document index for the symbol
        Downloades the index html with requests
        Throws html into beautiful soup -> validates the soup
        If page valid, returns a soup object
        '''
        link = settings.LINK_URL.format(self.ticker_symbol, q_or_k)
        r = requests.get(link)
        s = BS(r.text, "lxml")
        if self.validate_page(s):
            return s
        else:
            return False

    


    def get_10k_list(self):
        '''Retrieves 10-K SEC document index as soup
        Searches the soup for all relative paths for 10-K documents
        Generates full document URL and pushes it onto filings['10k_list']
        '''
        html = self.get_main_html('10-K')
        if html:
            for link in html.find_all('a', {'id': 'documentsbutton'}):
                doc_url = 'https://www.sec.gov' + link['href']
                self.filings['10k_list'].append(doc_url)
            # for link in html.find_all('a', {'id': 'interactiveDataBtn'}):
            #     doc_url = 'https://www.sec.gov' + link['href']
            #     self.filings['10k_xl_list'].append(doc_url)

    def get_html(self, html):
        s = BS(html, "lxml")
        try:
            html_link = s.find_all('table', {
                'class': 'tableFile',
                'summary': 'Document Format Files'
                })[0].find_all('tr')[1].find('a')['href']
            xtname = ['.html', '.htm']
            if os.path.splitext(html_link)[1] in xtname:
                html_link = 'https://www.sec.gov' + html_link
                # print(html_link)
                return html_link
            else:
                return False
        except IndexError:
            return False

    def get_date(self, html):
        # TODO Fix this unnecessary double calling of the soup
        s = BS(html, "lxml")
        try:
            date = s.find_all('div', {'class': 'formGrouping'})[
                1].find_all('div')[1].text
            return date
        except (IndexError, AttributeError):
            return False

    def get_all_10k(self):
        if len(self.filings['10k_list']) == 0:
            try:
                self.filings['errors']['10-K'].append('all')
            except (KeyError, AttributeError):
                self.filings['errors']['10-K'] = []
                self.filings['errors']['10-K'].append('all')
            return False
        dates = []
        errors = {}
        success = {}
        for link_val in self.filings['10k_list']:
            # for link_val in tqdm(self.filings['10k_list'], desc="10_klist"):
            r = requests.get(link_val)
            html_txt = r.text

            date = self.get_date(html_txt)
            dates.append(date)
            success[date] = []
            if settings.GET_HTML:
                link_html = self.get_html(html_txt)
                if link_html:
                    self.filings['10k_html'].append((link_html, date, 'html'))
                    try:
                        success[date].append('html')
                    except (KeyError, AttributeError):
                        success[date] = []
                        success[date].append('html')
                elif not link_html:
                    try:
                        errors[date].append('html')
                    except (KeyError, AttributeError):
                        errors[date] = []
                        errors[date].append('html')

        if len(errors) > 0:
            self.filings['errors']['count'] += 1
            self.filings['errors']['10-K'] = errors
        if len(success) > 0:
            self.filings['success']['count'] += 1
            self.filings['success']['10-K'] = success

    def check_duplicate(self, directory, filename):
        if filename in os.listdir(directory):
            return True
        return False

    def download_all(self):
        '''Puts download url and target directory into download_queue'''
        if settings.GET_HTML:
            for link in self.filings['10k_html']:
            # for link in tqdm(self.filings['10k_html'], desc="10-K"):
                fname = "{0}_{1}_{2}.html".format(
                    self.ticker_symbol, link[1], link[2])
                diry = '{0}/{1}/html/10-K/{2}/'.format(
                    settings.RAW_DATA_PATH, self.ticker_symbol, link[1])
                if not os.path.exists(diry):
                    os.makedirs(diry)
                if not self.check_duplicate(diry, fname):
                    download_request = (link[0], '{0}{1}'.format(diry, fname))
                    self.download_queue.put(download_request)
                    # print("Download Request", download_request)
                    # urllib.request.urlretrieve(
                    #     link[0], '{0}{1}'.format(diry, fname))


# import xml.etree.cElementTree as ET

# root = ET.fromstring(r.content)
# ns = {'role': 'http://www.w3.org/2005/Atom'}

# for entry in root.findall('role:entry/./role:content', ns):
#     filing_date = entry.find('role:filing-date', ns).text
#     filing_href = entry.find('role:filing-href', ns).text

# root = ET.fromstring(r.content)
# ns = {'role': 'http://www.w3.org/2005/Atom'}
# company = {'symbol': symbol, 'filings': []}
# for entry in root.findall('role:entry/./role:content', ns):
#     company['filings'].append({
#         'date': entry.find('role:filing-date', ns).text,
#         'href': entry.find('role:filing-href', ns).text
#     })
# xmlqueue.put(company)


# for i in range(5):
#     xml_thread_name = "XMLThread-[{}]".format(i+1)
#     xt = XMLThread(xml_thread_name, symbolqueue, xmlqueue)
#     xt.setDaemon(True)
#     xt.start()
#     print("Starting XML thread #{}".format(i+1))

# for i in range(2):
#     xml_print_thread_name = "XMLPrintThread-[{}]".format(i+1)
#     xpt = XMLPrintThread(xml_print_thread_name, xmlqueue)
#     xpt.setDaemon(True)
#     xpt.start()
#     print("Starting XML Print Thread #{}".format(i+1))



# class XMLThread(threading.Thread):
#     '''Gathers links from xml urls
#     Puts {symbol, date, filingurl} on filing scrape queue'''
#     def __init__(self, name, symbol_queue, xml_queue):
#         threading.Thread.__init__(self)
#         self.name = name
#         self.symbol_queue = symbol_queue
#         self.xml_queue = xml_queue

#     def run(self):
#         while True:
#             symbol = self.symbol_queue.get()
#             sc.scrape_xml_index(symbol)
#             self.symbol_queue.task_done()

# class XMLPrintThread(threading.Thread):
#     '''Gathers links from xml urls
#     Puts {symbol, date, filingurl} on filing scrape queue'''
#     def __init__(self, name, xml_queue):
#         threading.Thread.__init__(self)
#         self.name = name
#         self.xml_queue = xml_queue

#     def run(self):
#         while True:
#             company = self.xml_queue.get()
#             print("{}\t{} Left in XML Queue".format(company, self.xml_queue.qsize()))
#             self.xml_queue.task_done()
