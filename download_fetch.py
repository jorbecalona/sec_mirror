import itertools
from collections import Counter
from contextlib import closing

import requests
from bs4 import BeautifulSoup

srclink = "https://www.sec.gov/Archives/edgar/data/320193/000119312513416534/d590790d10k.htm"

r = requests.get(srclink)
soup = BeautifulSoup(r.content, 'lxml')

soup.find_all('a')

links = soup.select('a[href*="#"]')
targets = soup.select('a[name]')

links_names = [link.get('href').strip('#') for link in links]
targets_names = [target.get('name') for target in targets]

prod = list(itertools.product(links_names, targets_names))

prod = [(x,y) for x,y in prod if x == y]
prod_counter = Counter(prod).items()






s = requests.Session()

def streaming(symbols):
    payload = {'symbols': ','.join(symbols)}
    headers = {'connection': 'keep-alive', 'content-type': 'application/json', 'x-powered-by': 'Express', 'transfer-encoding': 'chunked'}
    req = requests.Request("GET",'https://stream.tradeking.com/v1/market/quotes.json',
                           headers=headers,
                           params=payload).prepare()

    resp = s.send(req, stream=True)

    for line in resp.iter_lines():
        if line:
            yield line


def read_stream():
    for line in streaming(['AAPL', 'GOOG']):
        print(line)


read_stream()





cik = '320193'
assension_number = '0001628280-16-020309'
assension_stripped = "".join(assension_number.split('-'))

sec_root = 'https://www.sec.gov/Archives/edgar/data/'
filing_root = "{root}/{cik}/{a_strip}/{a_raw}".format(
    root=sec_root, cik=cik, a_strip=assension_stripped, a_raw=assension_number)

filing_index = "{root}-index.htm".format(root=filing_root)
filing_txt = "{root}.txt".format(root=filing_root)


def filing_stream(cik, ass_num):
    # s = requests.Session()
    ass_strip = "".join(ass_num.split('-'))
    sec_root = 'https://www.sec.gov/Archives/edgar/data/'
    filing_root = "{root}/{cik}/{a_strip}/{a_raw}".format(
        root=sec_root, cik=cik, a_strip=ass_strip, a_raw=ass_num)
    filing_index = "{root}-index.htm".format(root=filing_root)
    filing_txt = "{root}.txt".format(root=filing_root)

    headers = {
        'connection': 'keep-alive',
        'content-type': 'application/json',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip',
        'transfer-encoding': 'chunked'
        }

    # with closing(s.send(req, stream=True)) as rerp:
    with closing(requests.get(filing_txt, stream=True)) as resp:
        for line in resp.iter_lines():
            if line:
                yield line

def capture_filename(cik_num, ass_num):
    maxlines = 300

    for count, line in enumerate(filing_stream(cik_num, ass_num)):
        linestr = str(line, encoding='utf-8')
        if '<FILENAME>' in linestr:
            print("Found filename on line {}".format(count))
            return linestr.lstrip('<FILENAME>').strip()
        elif count > maxlines:
            print("Too many lines, not enough filenames")
            return 0
        else:
            print("Line [{}]: {}".format(count, linestr))
            continue









    resp = s.send(req, stream=True)

    for line in resp.iter_lines():
        if line:
            yield line







    # Do things with the response here.


https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/0001628280-16-020309.txt


GET /Archives/edgar/data/320193/000162828016020309/0001628280-16-020309.txt HTTP/1.1
Host: www.sec.gov
Connection: keep-alive
Pragma: no-cache
Cache-Control: no-cache
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.15 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
Referer: https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/0001628280-16-020309-index.htm
Accept-Encoding: gzip, deflate, br
Accept-Language: en-US,en;q=0.8
Cookie: _ga=GA1.2.59113899.1489914080; fsr.s={"v":1}; fsr.a=1492779556151


cockboi = """<SEC-DOCUMENT>0001628280-16-020309.txt : 20161026
<SEC-HEADER>0001628280-16-020309.hdr.sgml : 20161026
<ACCEPTANCE-DATETIME>20161026164216
ACCESSION NUMBER:		0001628280-16-020309
CONFORMED SUBMISSION TYPE:	10-K
PUBLIC DOCUMENT COUNT:		96
CONFORMED PERIOD OF REPORT:	20160924
FILED AS OF DATE:		20161026
DATE AS OF CHANGE:		20161026

FILER:

	COMPANY DATA:	
		COMPANY CONFORMED NAME:			APPLE INC
		CENTRAL INDEX KEY:			0000320193
		STANDARD INDUSTRIAL CLASSIFICATION:	ELECTRONIC COMPUTERS [3571]
		IRS NUMBER:				942404110
		STATE OF INCORPORATION:			CA
		FISCAL YEAR END:			0924

	FILING VALUES:
		FORM TYPE:		10-K
		SEC ACT:		1934 Act
		SEC FILE NUMBER:	001-36743
		FILM NUMBER:		161953070

	BUSINESS ADDRESS:	
		STREET 1:		ONE INFINITE LOOP
		CITY:			CUPERTINO
		STATE:			CA
		ZIP:			95014
		BUSINESS PHONE:		(408) 996-1010

	MAIL ADDRESS:	
		STREET 1:		ONE INFINITE LOOP
		CITY:			CUPERTINO
		STATE:			CA
		ZIP:			95014

	FORMER COMPANY:	
		FORMER CONFORMED NAME:	APPLE COMPUTER INC
		DATE OF NAME CHANGE:	19970808
</SEC-HEADER>
<DOCUMENT>
<TYPE>10-K
<SEQUENCE>1
<FILENAME>a201610-k9242016.htm
<DESCRIPTION>10-K
<TEXT>"""