"""
item_parser.py

Jonathan Poczatek
5/11/2017
"""

import findspark
from bs4 import BeautifulSoup
from functional import seq

findspark.init()
from pyspark import SparkContext

# from fn import _
# import bs4
# import itertools

hdfs_ip = '10.0.0.90'
sc = SparkContext(appName="ScraXBRL")
local_files_dir = 'file:///Users/jonathanpoczatek/Documents/Code/ScraXBRL/data/mnt/disks/aux/ScraXBRL/data/*html*'
hdfs_files_dir = 'hdfs://localhost:9000/scraxbrl/data/*'
item_keys = ['item1', 'item1a', 'item1b', 'item2', 'item3', 'item4', 'item5', 'item6',
            'item7', 'item7a', 'item8', 'item9', 'item9a', 'item9b', 'item10', 'item11', 'item12',
            'item13', 'item14', 'item15']
item_keys.reverse()

def formatHTMLItemDivs(x):
    """
    Its dank. Takes an html string
    Returns a parsed html string with item lables written as div IDs
    Also, all items are assigned a class 'item'
    """
    soup = BeautifulSoup(x, "lxml")
    targets = soup.select('a[name]')
    assert len(targets) != 0

    def getTargetName(target_elt):
        rslt = {
            'target_elt': target_elt,
            'target_name': target_elt.get('name')
        }
        return rslt

    def getLinkElt(rslt):
        rslt['link_elt'] = soup.find('a', attrs={'href': "#{}".format(rslt['target_name'])})
        return rslt

    def getLinkRowElt(rslt):
        rslt['link_tr_elt'] = rslt['link_elt'].findParent('tr')
        return rslt

    def getLinkRowTextMin(rslt):
        row_text = rslt['link_tr_elt'].text
        rslt['row_text_min'] = "".join(row_text.split()).lower()
        return rslt

    def getItemTag(rslt):
        for key in item_keys:
            if key in rslt['row_text_min']:
                rslt['item_lbl'] = key
                return rslt

    def addItemAttrs(rslt):
        item = rslt['target_elt']
        item['class'] = 'item'
        item['id'] = rslt['item_lbl']
        return rslt

    item_logs = (
        seq(targets)
        .map(getTargetName)
        .map(getLinkElt)
        .map(getLinkRowElt)
        # .filter(lambda x: isinstance(x['link_tr_elt'], bs4.element.Tag))
        .filter(lambda x: x != None)
        .map(getLinkRowTextMin)
        .map(getItemTag)
        .filter(lambda x: x != None)
        .map(addItemAttrs)
        # .map(wrapItemSection)
        # .to_list()
    )
    return str(soup)


from BeautifulSoup import 
    # def wrapItemSection(rslt):
    #     # Not used
    #     def end_tag_rule(start_tag_name, sib_elt):
    #         try:
    #             print(sib_elt)
    #             if start_tag_name == sib_elt.attrs.get('name'):
    #                 print('same element')
    #                 return True
    #             else:
    #                 return sib_elt.attrs.get('class') != 'target'# and sib_elt is not None
    #         except:
    #             return False
    #         # return sib_elt['class'] != 'item' and sib_elt is not None
    #     start_tag = rslt['target_elt']
    #     try:
    #         end_tag = start_tag.find_next('div', attrs={'class': 'item'}
    #     elts = [i for i in itertools.takewhile(lambda x: end_tag_rule(start_tag.attrs.get('name'), x), start_tag.nextGenerator)]
    #     print(elts)
    #     itertools.take
    #     while 
    #     item_div = soup.new_tag('div')
    #     item_div.attrs = {
    #         'class': 'item',
    #         'id': rslt['item_lbl']
    #     }
    #     start_tag.wrap(item_div)
    #     for element in elts:
    #         item_div.append(element)
    #     rslt['item_div_created'] = True
    #     return rslt

    



# soup.find('a', attrs={'href': "#{}".format(targets[2].get('name'))}).findParent('tr').text

# def parse_10k(X):
#     pass
    # # Remove Unicode and shit
    # # Soup the nigga up
    # # Grab Metadata
    # # Get links and targets
    # # Get tables
    # soup = BeautifulSoup(X, "lxml")
    # tables = soup.select('table')
    # targets = soup.select('a[name]')

    # # If not targets, we fucked
    # if not targets:
    #     print('No Links')
    #     print('Generating Item Links')
    #     # return a new soup with targets

    # assert(len(targets) != 0)

    # # Re-structure HTML with Item Divs
    # formatHTMLItemDivs()

    # {
    #     'item_logs': item_logs,
    #     'parsed_html': str(soup)
    # }



# def main():
#     files = sc.textFile(local_files_dir)
#     hdfs_files = sc.textFile(hdfs_files_dir)
#     # hdfs_remote_files = sc.textFile(hdfs_remote_files_dir)
#     # parsed = files.map(parse_10k)
