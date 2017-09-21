"""
sec_datafabric.py

Jonathan Poczatek
4/21/2017
"""


import pandas as pd
import requests
from bs4 import BeautifulSoup

import datafabric_settings as settings
import xmltodict
import json
import collections
# from lxml.html import Element, fromstring
# from xmljson import parker, Parker
from json import dumps
import xmltodict

# Test if file exists


def getFeedUrl(cik):
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=10-K&dateb=&owner=exclude&start=0&count=100&output=atom".format(cik)


def getDefaultFeedUrl(cik):
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=exclude&start=0&count=100&output=atom".format(cik)


# df = pd.read_csv(settings.CIK_LIST_CSV)
# store = pd.HDFStore(settings.LOCAL_HDF)

# lines = []
with open(settings.CIK_LIST_PRASANNA, 'r', encoding='latin-1') as f:
    lines = [x.split(':')[1] for x in f.readlines()]


lines = list(set(lines))
lines = lines[1:]

# map(lambda x: getDefaultFeedUrl(x), lines)
cik_xml = [{'CIK': cik, 'FEED': getDefaultFeedUrl(cik)} for cik in lines]

# store.put('cik_xml', cik_xml)


namespaces = {
    "http://www.w3.org/2005/Atom": None
}
# xmldict = xmltodict.parse(xml, process_namespaces=True, namespaces=namespaces)
# print(json.dumps(xmldict, indent=4))


def parse_xml(feed_url, parsed_dict=None):
    """
    Download
    """
    resp = requests.get(feed_url)
    raw_dict = xmltodict.parse(
        resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=namespaces)

    def flatten_links(xmldict):
        links = {}
        for x in xmldict['feed']['link']:
            links[x['@rel']] = {
                'type': x['@type'],
                'href': x['@href']
            }
        return links

    def get_next(xmldict):
        return [x['@href'] for x in xmldict['feed']['link'] if x['@rel'] == 'next']

    def get_filings(xmldict):
        return [x for x in xmldict['feed']['entry']]

    if get_next(raw_dict):
        links = []
    links.append(feed_url)
    resp = requests.get(feed_url)
    soup = BeautifulSoup(resp.content, 'xml')

    # if x['@rel'] == 'next':

    entries = soup.find_all('entry')
    page_next = soup.select('link[rel="next"]')
    entries = soup.find_all('entry')
    # print('Added [{}] entries'.format(len(entries)))

    if page_next:
        # print('Page_Next')
        return parse_xml()(page_next[0].get('href'), links)
    else:
        return links


def listings(CIK):
    feed_url = getDefaultFeedUrl(CIK)

    resp = requests.get(feed_url)
    raw_dict = xmltodict.parse(
        resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=namespaces)
    links = flatten_links(raw_dict)

    def flatten_links(xmldict):
        links = {}
        for x in xmldict['feed']['link']:
            links[x['@rel']] = {
                'type': x['@type'],
                'href': x['@href']
            }
        return links


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


# from xml.etree.ElementTree import fromstring
def flatten_links(xmldict):
    links = {}
    for x in xmldict['feed']['link']:
        links[x['@rel']] = {
            'type': x['@type'],
            'href': x['@href']
        }
    return links

# 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&type=&datea=&dateb=&owner=exclude&count=100&output=atom&start=100'
# xmldict['keys'] ['author', 'company-info', 'entry', 'id', 'link', 'title', 'updated']
