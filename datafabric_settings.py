"""
datafabric_settings.py

Jonathan Poczatek
4/21/2017
"""


################################
## Data Files and Directories ##
################################

#Data Locations
MAIN_DATA_DIR = 'data'
RAW_DATA_DIR = 'raw_data'
EXTRACTED_DATA_DIR = 'extracted_data'


#Path Names
RAW_DATA_PATH = "{0}/{1}".format(MAIN_DATA_DIR, RAW_DATA_DIR)
EXTRACTED_DATA_PATH = "{0}/{1}".format(MAIN_DATA_DIR, EXTRACTED_DATA_DIR)

######################################################
## Scrape Lists and Reference Files and Directories ##
######################################################

#Scrape List Locations
MAIN_SCRAPE_LIST_DIR = 'scrape_lists'
STOCK_EXCHANGE_LIST_DIR = 'stock_exchanges'

CONFIG_RES_DIR = 'config'
CIK_LIST_CSV = '{0}/investment_company_series_class.csv'.format(CONFIG_RES_DIR)
CIK_LIST_PRASANNA = '{0}/cik.coleft.c.txt'.format(CONFIG_RES_DIR)

LOCAL_HDF = '{0}/local_store.h5'.format(CONFIG_RES_DIR)

OTHER_SCRAPE_LIST_DIR = 'other_scrapes'

#####################
## Scraper Options ##
#####################
GET_XML = False
GET_TXT = False
GET_HTML = True
GET_XL = False

#####################
## Extract Options ##
#####################
OUTPUT_PICKLE = True
OUTPUT_JSON = False

###########
## URLs ###
###########

LINK_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type={1}&dateb=&owner=exclude&count=100"

RSS_XML_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type={1}&dateb=&owner=exclude&start=0&count=100&output=atom"
