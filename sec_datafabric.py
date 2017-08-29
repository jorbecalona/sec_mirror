"""
sec_datafabric.py

Jonathan Poczatek
4/21/2017
"""


import pandas as pd
import requests
import datafabric_settings as settings
from bs4 import BeautifulSoup
import requests
import xmltodict


# Test if file exists
def getFeedUrl(cik):
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=10-K&dateb=&owner=exclude&start=0&count=100&output=atom".format(cik)


def getDefaultFeedUrl(cik):
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=exclude&start=0&count=100&output=atom".format(cik)


df = pd.read_csv(settings.CIK_LIST_CSV)
store = pd.HDFStore(settings.LOCAL_HDF)

# lines = []
with open(settings.CIK_LIST_PRASANNA, 'r', encoding='latin-1') as f:
    lines = [x.splitx.split(':')[1] for x in f.readlines()]


lines = list(set(lines))
lines = lines[1:]

# map(lambda x: getDefaultFeedUrl(x), lines)
cik_xml = [{'CIK': cik, 'FEED': getDefaultFeedUrl(cik)} for cik in lines]

# store.put('cik_xml', cik_xml)


def parse_feed(feed_url, links=None):
    if not links:
        links = []
    links.append(feed_url)
    resp = requests.get(feed_url)
    soup = BeautifulSoup(resp.content, 'xml')

    entries = soup.find_all('entry')
    page_next = soup.select('link[rel="next"]')
    entries = soup.find_all('entry')
    # print('Added [{}] entries'.format(len(entries)))
    if page_next:
        # print('Page_Next')
        return parse_feed(page_next[0].get('href'), links)
    else:
        return links


true_index = [12, 56, 140, 158, 167, 177, 183,
              244, 338, 342, 350, 361, 371, 394, 453, 496]

testfeeds = ['https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000898293&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001102578&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000817632&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001329517&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000351917&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001321544&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000851968&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001101026&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001491419&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000022082&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000832820&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001025148&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000888693&type=&dateb=&owner=exclude&start=0&count=100&output=atom',
             'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000825542&type=&dateb=&owner=exclude&start=0&count=100&output=atom']

test = xmltodict.parse(requests.get(testfeeds[0]).content)
len(test['feed']['entry'])


# 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&type=&datea=&dateb=&owner=exclude&count=100&output=atom&start=100'


# dict_keys(['@xmlns', 'author', 'company-info',
#    'entry', 'id', 'link', 'title', 'updated'])

# Get author

# Get company-info
# addresses
# address
# type
# city
# state
# street
# zip


# Get entry

# Get id

# Get link

# Get title

# Get updated


# unique_ciks = pd.Series.from_array(lines)

# Test if exists

# store.put('companies', df)

# s2 = s.map(lambda x: 'this is a string {}'.format(x),
#                na_action=None)

# "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=10-K&dateb=&owner=exclude&start=0&count=100&output=atom"


# Handle pagenation

# mapped = pd.DataFrame({'CIK': (pd.Series(df['CIK'].unique())),
#         'XMLURL': (pd.Series(df['CIK'].unique())).map(lambda x: getDefaultFeedUrl(x), na_action=None)
# })

# mapped = pd.DataFrame({'CIK': unique_ciks,
#         'XMLURL': unique_ciks.map(lambda x: getDefaultFeedUrl(x), na_action=None)
# })

# store.put('mapped', mapped)


# r = requests.get()

# grab all entries
