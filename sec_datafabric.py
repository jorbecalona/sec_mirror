"""
sec_datafabric.py

Jonathan Poczatek
4/21/2017
"""


# import collections
import json
# from lxml.html import Element, fromstring
# from xmljson import parker, Parker
# from json import dumps

# import pandas as pd
import requests
import xmltodict
# from bs4 import BeautifulSoup

from datafabric_settings import *

# import xmltodict

# Test if file exists


# def getFeedUrl(cik):
#     return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=10-K&dateb=&owner=exclude&start=0&count=100&output=atom".format(cik)


def getDefaultFeedUrl(cik, start_index=0):
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=exclude&count=100&output=atom&start={1}".format(cik, start_index)


# df = pd.read_csv(settings.CIK_LIST_CSV)
# store = pd.HDFStore(settings.LOCAL_HDF)

# lines = []

# map(lambda x: getDefaultFeedUrl(x), lines)
# cik_xml = [{'CIK': cik, 'FEED': getDefaultFeedUrl(cik)} for cik in lines]

# store.put('cik_xml', cik_xml)
# xmldict = xmltodict.parse(xml, process_namespaces=True, namespaces=namespaces)
# print(json.dumps(xmldict, indent=4))


def get_cik_list():
    with open(CIK_LIST_PRASANNA, 'r', encoding='latin-1') as f:
        lines = [x.split(':')[1] for x in f.readlines()]

    lines = list(set(lines))
    return lines[1:]


def get_next(xmldict):
    for x in xmldict['link']:
        if x['@rel'] == 'next':
            return x.get('@href')


def company_listing_dict(cik):
    """
    Returns a fucking parsed dict
    """
    # Company Entry Dict Template
    company_entry = {'entry': []}

    # Generate XML Feed URL from CIK
    feed_url = getDefaultFeedUrl(cik)

    # Download Feed
    resp = requests.get(feed_url)
    assert resp.status_code == 200

    # Parse XML to Dict
    raw_dict = xmltodict.parse(
        resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=NAMESPACES)['feed']

    # Obtain Company-Info
    assert raw_dict.get('company-info')
    company_entry['company-info'] = raw_dict.get('company-info')
    # Obtain Entries
    assert raw_dict.get('entry')
    company_entry['entry'].extend(raw_dict.get('entry'))

    page_next_url = get_next(raw_dict)
    while page_next_url:
        # Download Feed
        resp = requests.get(page_next_url)
        assert resp.status_code == 200

        # Parse XML to Dict
        raw_dict = xmltodict.parse(
            resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=NAMESPACES)['feed']

        assert raw_dict.get('entry')
        company_entry['entry'].extend(raw_dict.get('entry'))

        page_next_url = get_next(raw_dict)

    return company_entry


# def parse_xml(feed_url, parsed_dict=None):
#     """
#     Download
#     """
#     resp = requests.get(feed_url)
#     raw_dict = xmltodict.parse(
#         resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=namespaces)

#     def flatten_links(xmldict):
#         links = {}
#         for x in xmldict['feed']['link']:
#             links[x['@rel']] = {
#                 'type': x['@type'],
#                 'href': x['@href']
#             }
#         return links

#     def get_next(xmldict):
#         return [x['@href'] for x in xmldict['feed']['link'] if x['@rel'] == 'next']
#         for x in xmldict['link']:
#             if x['@rel'] == 'next']:
#                 return x['@href']

#     def get_next3(xmldict):
#         for x in xmldict['link']:
#             if x['@rel'] == 'next':
#                 return x['@href']

#     def get_filings(xmldict):
#         return [x for x in xmldict['feed']['entry']]

#     if get_next(raw_dict):
#         links= []
#     links.append(feed_url)
#     resp= requests.get(feed_url)
#     soup= BeautifulSoup(resp.content, 'xml')

#     # if x['@rel'] == 'next':

#     entries= soup.find_all('entry')
#     page_next= soup.select('link[rel="next"]')
#     entries= soup.find_all('entry')
#     # print('Added [{}] entries'.format(len(entries)))

#     if page_next:
#         print('Page_Next')
#         # return parse_xml()(page_next[0].get('href'), links)
#     else:
#         return links

# def listings(CIK):
#     """
#     Params: Company CIK string
#     Output:
#     """
#     def flatten_links(xmldict):
#         links_dict={}
#         for x in xmldict['feed']['link']:
#             links_dict[x['@rel']]={
#                 'type': x['@type'],
#                 'href': x['@href']
#             }
#         return links_dict

#     # Generate XML Feed URL from CIK
#     feed_url = getDefaultFeedUrl(CIK)

#     # Download Feed
#     resp = requests.get(feed_url)

#     # Parse XML to Dict
#     raw_dict = xmltodict.parse(
#         resp.content, encoding = "ISO-8859-1", process_namespaces = True, namespaces = namespaces)
#     # ['author', 'company-info', 'entry', 'id', 'link', 'title', 'updated'
#     # Parse out the company header and store
#     # Parse out the entries and store in dict
#     # Pagenation Links Logic
#     # Parse pagenation links

#     # links = flatten_links(raw_dict)
#     # return links


# def parse_feed(feed_url, links = None):
#     if not links:
#         links=[]
#     links.append(feed_url)
#     resp=requests.get(feed_url)
#     soup=BeautifulSoup(resp.content, 'xml')
#     entries=soup.find_all('entry')
#     page_next=soup.select('link[rel="next"]')
#     # entries = soup.find_all('entry')
#     # print('Added [{}] entries'.format(len(entries)))
#     if page_next:
#         # print('Page_Next')
#         return parse_feed(page_next[0].get('href'), links)
#     else:
#         return links


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

testciks = ['0001260507',
            '0000898293',
            '0001102578',
            '0000817632',
            '0001329517',
            '0000351917',
            '0001321544',
            '0000851968',
            '0001101026',
            '0001491419',
            '0000022082',
            '0000832820',
            '0001025148',
            '0000888693',
            '0000825542'
            ]


def test_run(ciks=testciks):

    # test = xmltodict.parse(requests.get(testfeeds[0]).content)
    # len(test['feed']['entry'])

    # from xml.etree.ElementTree import fromstring
    # def flatten_links(xmldict):
    #     links = {}
    #     for x in xmldict['feed']['link']:
    #         links[x['@rel']] = {
    #             'type': x['@type'],
    #             'href': x['@href']
    #         }
    #     return links

    # 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&type=&datea=&dateb=&owner=exclude&count=100&output=atom&start=100'
    # xmldict['keys'] ['author', 'company-info', 'entry', 'id', 'link', 'title', 'updated']
