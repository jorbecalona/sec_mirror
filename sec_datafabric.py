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

import pandas as pd
import requests
import xmltodict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
    company_entry = {'cik': cik, 'entries': []}

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
    company_entry['entries'].extend(raw_dict.get('entry'))

    page_next_url = get_next(raw_dict)
    while page_next_url:
        # Download Feed
        resp = requests.get(page_next_url)
        assert resp.status_code == 200

        # Parse XML to Dict
        raw_dict = xmltodict.parse(
            resp.content, encoding="ISO-8859-1", process_namespaces=True, namespaces=NAMESPACES)['feed']

        assert raw_dict.get('entry')
        company_entry['entries'].extend(raw_dict.get('entry'))

        page_next_url = get_next(raw_dict)

    return company_entry


def jsonify_company(company_entry):
    entries = json.dumps(company_entry['entries'])
    company = json.dumps(company_entry['company-info'])
    return {'cik': company_entry['cik'],
            'company-info': company,
            'entries': entries}


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


def test_run(cik_list, company_listings):
    # ciks = get_cik_list()
    # store = pd.HDFStore(LOCAL_HDF)
    # company_listings = {}
    for cik in cik_list:
        print(f'processing  {cik}')
        listing = company_listing_dict(cik)
        company_listings[cik] = jsonify_company(listing)
    return company_listings
    # store.put('company_listings', company_listings)


store = pd.HDFStore(LOCAL_HDF)
company_listings = {}
test_run(testciks, company_listings)


engine = create_engine('postgresql://flaskapp:hunter2@localhost:5432/flaskapp')
Session = sessionmaker(bind=engine)
session = Session()


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
