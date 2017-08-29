import re
import os
import pandas as pd
import itertools
from bs4 import BeautifulSoup
from bs4.element import NavigableString as bs4_navstring
from scrapy.selector import Selector
import string
import unicodedata
from difflib import SequenceMatcher as SM
from apply_parallel import apply_parallel as ap
import html
from dask.diagnostics import ProgressBar

def get_num_to_alpha_ratio(textStr):
    num_numeric = sum(c.isdigit() for c in textStr)
    num_alpha = sum(c.isalpha() for c in textStr)
    num_alpha_ratio = num_numeric / (num_numeric + num_alpha)
    return num_alpha_ratio

#cleaning/filtration functions (return modified input string):
def strip_markup_tags(text, rep = '', detect_breaks = True):
    if detect_breaks:
        dummy = '==zzzzz=='
        break_strs = ['<page>', '<p>', '<br>', '</div>']
        for break_str in break_strs:
            text = re.sub(break_str, dummy, text, flags=re.IGNORECASE)
        text = re.sub('<[^<]+?>', rep, text)
        text = re.sub(dummy, ' ', text)
        return text
    else:
        return re.sub('<[^<]+?>', rep, text)

def strip_markup_tags2(text, rep='', ignore_item_tags=True):
    if ignore_item_tags:
        tags = list(set(re.findall('<[^<]+?>', text)))
        for tag in tags:
            if 'div' not in tag:
                text = text.replace(tag, '')
        return text
    else:
        return re.sub('<[^<]+?>', rep, text)

def decode_html_chars(text):
    sub_dict = {
        '(&nbsp;|&#160;|&#09;|&#10;|&#32;)': ' ', #spaces
        '(&quot;|&#34;|&lsquo;|&rsquo;|&ldquo;|&rdquo;|&#145;|&#146;|&#147;|&#148;|&#8217;|&#822[01];)': '"', #quotes
        '(&amp;|&#38;)':'&', #ampersand
        '(&apos;|&#39;)': "'", #apostraphe
        '(&lt;|&#60;)': ' LT ', #less than sign
        '(&gt;|&#62;)': ' GT ', #greater than sign
        '(&#821[12];)' : '-', #dashes
        '(&#8226;|&#9702;)': '', #bullets
        '(&#185;)' : '', #references
    }
    for reg_str, sub_str in sub_dict.items():
        text = re.sub(reg_str, sub_str, text, flags = re.IGNORECASE)
    return text

def get_tag_iterator(text_str, tag, include_tags=False, return_match_objs=False):
    if include_tags:
        regStr = '(<{tag}>.+?</{tag}>)'.format(tag=tag)
    else:
        regStr = '(?<=<{tag}>)(.+?)(?=</{tag}>)'.format(tag=tag)

    if return_match_objs:
        return re.finditer(regStr, text_str, re.I | re.DOTALL)
    else:
        return map(lambda x: x.group(0), re.finditer(regStr, text_str, re.I | re.DOTALL))

def filter_html_tables(text_str, max_num_alpha_ratio=0.15):
    table_iter = get_tag_iterator(text_str, 'table', return_match_objs= True)
    for table_match_obj in table_iter:
        table_str = table_match_obj.group(0)
        table_start_ind = table_match_obj.start(0)
        table_end_ind = table_match_obj.end(0)

        item78_check = re.search('item([\s.]{0,4}|<[^<]+?>)[78]', table_str, re.IGNORECASE) is not None
        ratio_check = (get_num_to_alpha_ratio(strip_markup_tags(table_str, detect_breaks = False)) <= max_num_alpha_ratio)
        if item78_check or ratio_check:
            #continue #aka do not remove table from string
            before = text_str[0:table_start_ind]
            after = text_str[table_end_ind::]
            out_str = before+' '+strip_markup_tags(table_str)+' '+after #replace all markup within tables that are kept with space
            text_str = out_str
        else:
            before = text_str[0:table_start_ind]
            after = text_str[table_end_ind::]
            out_str = before+' '+after
            text_str = out_str
    return text_str

def clean_anomalies(text_str):
    text_str = re.sub('(-[\n\r\t\v])', '-', text_str) #replace dashes followed by a newline, carriage return, or tab with a dash
    text_str = re.sub('(&[\S]{2,6};)', '', text_str) #remove all remaining html chars starting with &
    text_str = re.sub('(#[a-f0-9]{6})', '', text_str) #remove all remaining html chars starting with #

    #remove other anomalies:
    text_str = re.sub('(\s-\s)', ' ', text_str)
    text_str = re.sub('and/or', 'and or', text_str, flags=re.IGNORECASE)
    text_str = re.sub('(([-.=]){2,}\s*)', ' ', text_str)
    text_str = re.sub('_', '', text_str)
    text_str = re.sub('(\s){3,}', ' ', text_str)
    text_str = re.sub('(\n\s*?){3,}', '\n\n', text_str)
    text_str = re.sub('(?<![\n])(\n)(?![\n\s])', ' ', text_str)
    return text_str

def clean_sec_html_str(text_str, max_num_alpha_ratio=0.15):
    raw_doc_text = text_str
    try:
        if raw_doc_text:
            raw_doc_text = filter_html_tables(raw_doc_text, max_num_alpha_ratio= max_num_alpha_ratio) #filter tables
            raw_doc_text = strip_markup_tags(raw_doc_text) #remove remaining markup
            raw_doc_text = html.unescape(raw_doc_text) #unescape html chars
            #raw_doc_text = decode_html_chars(raw_doc_text) #decode html chars
            raw_doc_text = clean_anomalies(raw_doc_text) #clean anomalies
            return raw_doc_text
        else:
            return False
    except:
        print('problem cleaning string')
        return False

def read_html(html_path, n_bytes = None):
    fhandle = open(html_path, 'r')
    if n_bytes == None:
        fileContents = fhandle.read()
    else:
        fileContents = fhandle.read(n_bytes)
    return fileContents

def get_all_txt_filepaths(mypath):
    filepaths = []
    for root, dirs, files in os.walk(mypath):
        for file in files:
            if file.endswith('.txt'):
                filepaths.append(os.path.join(root, file))
    return filepaths

def get_all_html_filepaths(mypath):
    filepaths = []
    for root, dirs, files in os.walk(mypath):
        for file in files:
            if file.endswith('.htm'):
                filepaths.append(os.path.join(root, file))
    return filepaths


def write_to_file(html_str, fpath):
    with open(fpath, "w") as fhandle:
        fhandle.write(html_str)
    return

def preprocess_10k(html_str):
    html_str = clean_sec_html_str(html_str)
    html_str = html_str.replace('/n', ' ')
    return html_str


item_labels = ['item_1', 'item_1a', 'item_1b', 'item_2', 'item_3', 'item_4', 'item_5', 'item_6',
               'item_7', 'item_7a', 'item_8', 'item_9', 'item_9a', 'item_9b', 'item_10', 'item_11', 'item_12',
               'item_13', 'item_14', 'item_15']

item_titles = ['business', 'risk_factors', 'unresolved_staff_comments', 'properties', 'legal_proceedings',
               'mine_safety_disclosures',
               'market_for_registrants_common_equity_related_stockholder_matters_and_issuer_purchases_of_equity_securities',
               'selected_financial_data',
               'managements_discussion_and_analysis_of_financial_condition_and_results_of_operations',
               'quantitative_and_qualitative_disclosures_about_market_risk',
               'financial_statements_and_supplementary_data',
               'changes_in_and_disagreements_with_accountants_on_accounting_and_financial_disclosure',
               'controls_and_procedures', 'other_information',
               'directors_executive_officers_and_corporate_governance',
               'executive_compensation',
               'security_ownership_of_certain_beneficial_owners_and_management_and_related_stockholder_matters',
               'certain_relationships_and_related_transactions_and_director_independence',
               'principal_accountant_fees_and_services', 'exhibits_financial_statement_schedules']


class ParseError(Exception):
    pass
    """Raise for html Parsing Error"""


def open_file(fp):
    with open(fp, 'r') as fhandle:
        contents = fhandle.read()
    return unicodedata.normalize('NFKD', contents).encode('ascii', 'ignore')

def tag_checker(cur_tag, end_tag):
    try:
        if type(cur_tag) == bs4_navstring:
            return True
        if cur_tag.has_attr('name'):
            return cur_tag.attrs.get('name') != end_tag.attrs.get('name')
        else:
            return True
    except:
        return False


###########################

def normalize_elt(elt, alphanum=True):
    """
    Normalize string by removing newlines, punctuation, spaces,
    and optionally filtering for alphanumeric chars

    Args:
        elt (string):
            string to normalize
        alphanum (bool, optional, default True):
            if True, only return elt if it contains at least
            one alphanumeric char, return None otherwise

    Returns:
        norm_elt (string):
            normalized string or None
    """
    norm_elt = elt.replace('\n', '')  # remove new lines
    translator = str.maketrans('', '', string.punctuation)
    norm_elt = norm_elt.lower().translate(translator)  # lowercase then remove punctuation
    norm_elt = norm_elt.strip().replace(' ', '_')  # replace spaces with underscores
    if alphanum:
        alphanum_check = re.search('[a-zA-Z0-9]', norm_elt)
        if alphanum_check:
            return norm_elt
        else:
            return None
    else:
        return norm_elt

def normalize_elts(elts, alphanum=True):
    """
    Normalize list of strings by calling 

    Args:
        elts (list): 
            list of strings to normalize
        alphanum (bool, optional, default True): 
            if True, only return elts that contains at least one alphanumeric char, return None otherwise

    Returns:
        (list): returns all elements that are not None
            
    """
    row_elts = list(map(lambda x: normalize_elt(x, alphanum=alphanum), elts))
    return [x for x in row_elts if x] #get all elements that are not None

def get_unique_elts(seq, keep_left_most=True):
    """
    Get unique elements of list (seq) whilst preserving order

    Args:
        seq (iterable): 
            iterable of hashable objects
        keep_left_most (bool, optional, default True): 
            if True, keep the left-most (aka the first occurring) element when there are repeats, otherwise keep right-most

    Returns:
        (list): list from seq with repeats removed

    """
    seen = set()
    seen_add = seen.add
    if keep_left_most:
        return [x for x in seq if not (x in seen or seen_add(x))]
    else:
        return list(reversed([x for x in reversed(seq) if not (x in seen or seen_add(x))]))

def get_parsed_items(html_str, fuzzy_threshold=0.8, get_marked_html=False):
    # 1. find table of contents rows in html string
    sel = Selector(text=html_str, type='html')
    table_row_path = '//table//tr[(td//text()[re:match(.,"item","i")]) and (td//a[contains(@href,"#")])]'
    toc_rows = sel.xpath(table_row_path)
    if not toc_rows:
        print('no links found')
        return False

    # 2. find text of rows and the first occuring link in each row (there should only be one unique link per row)
    toc_rows_text = [get_unique_elts(x.xpath('.//text()[re:match(.,"[a-zA-Z_]")]').extract()) for x in toc_rows]
    toc_rows_text = list(map(normalize_elts, toc_rows_text))
    toc_rows_links = [get_unique_elts(x.xpath('.//a/@href').extract())[0] for x in toc_rows]  # guaranteeing one link per row with [0]

    # 3. determine each row's item label and title
    toc_rows2 = []
    for row_elts, row_link in reversed(list(zip(toc_rows_text, toc_rows_links))):  # start from item 15 and go to item 1
        row_dict = {'label': None, 'title': None, 'link': None, 'next_link': None}
        key_match = list(set(row_elts) & set(item_labels))
        val_match = list(set(row_elts) & set(item_titles))
        if key_match:  # first try to get exact matches on item labels
            row_dict['label'] = key_match[0]
            row_dict['title'] = item_titles[item_labels.index(key_match[0])]
        elif val_match:  # then try to get exact matches on item titles
            row_dict['label'] = item_labels[item_titles.index(val_match[0])]
            row_dict['title'] = val_match[0]
        elif fuzzy_threshold < 1:
            # if no exact matches can be found and
            # fuzzy threshold is less than 1:
            #   perform fuzzy matching on item titles:
            poss_matches = list(itertools.product(row_elts, item_titles))
            sims = [SM(None, elt, title).ratio() for elt, title in poss_matches]
            max_sim = max(sims)
            if max_sim >= fuzzy_threshold:  # fuzzy matching measurement
                item_title = poss_matches[sims.index(max_sim)][1]
                row_dict['label'] = item_labels[item_titles.index(item_title)]
                row_dict['title'] = item_title
        if row_dict['label'] and row_dict['title']:  # if found, assign links and append
            row_dict['link'] = row_link
            if toc_rows2:
                row_dict['next_link'] = toc_rows2[-1]['link']
            else:
                row_dict['next_link'] = None
            toc_rows2.append(row_dict)
    toc_rows2 = list(reversed(toc_rows2))  # change back to ascending order (item 1 first)

    # 4. check if all items are present
    toc_rows2_labels = [x['label'] for x in toc_rows2]
    if set(toc_rows2_labels) != set(item_labels):
        print('not all items found')
        print('the following items are missing: ', str(list(set(item_labels) - set(toc_rows2_labels))))
        return False

    # 5. find html tags for each item:
    soup = BeautifulSoup(html_str, 'lxml')
    tag = None
    for row_dict in reversed(toc_rows2):
        row_dict.update({'next_tag': tag})
        tag = soup.find('a', attrs={'name': row_dict['link'].replace('#', '')})
        row_dict.update({'tag': tag})

    # 6. update soup with new sections and extract html for each item:
    for row_dict in toc_rows2:
        next_elts = list(row_dict['tag'].next_elements)
        els = [x for x in itertools.takewhile(lambda y: tag_checker(y, row_dict['next_tag']), next_elts)]
        section = soup.new_tag('div')
        section.attrs = {'class': 'marked_item', 'id': row_dict['label']}
        row_dict['tag'].wrap(section)
        for tag in els:
            section.append(tag)
        extracted_html = soup.find('div', attrs=section.attrs)
        row_dict.update({'html': str(extracted_html)})

    if get_marked_html:
        new_html_str = str(soup)
        return new_html_str
    else:
        return toc_rows2

def parse_clean_write(filename, html_file_dir_path, html_parsed_dir_path):
    print('processing ', filename)
    parsed = get_parsed_items(read_html(html_file_dir_path+filename))
    if parsed:
        combos = [(item['html'], item['label']) for item in parsed]
        dirname = html_parsed_dir_path + filename.replace('.htm', '')
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        for item_html, label in combos:
            item_html = preprocess_10k(str(item_html))
            new_fname = filename.replace('.htm', '_'+label+'.htm')
            write_to_file(item_html, dirname+'/'+new_fname)
        print('successfully parsed the following file: ', filename)
        print()
        return True
    else:
        print('the following file is not parsable deterministically: ', filename)
        print()
        return False

def parallel_pcw(dfrow):
    filename = dfrow['localPath_html'].split('/')[-1]
    try:
        return parse_clean_write(filename)
    except:
        print('an unknown error occurred when parsing the following file: ', filename)
        print()
        return False

def main():
    spec = pd.read_csv('data_repo/10k_sample/spec.csv')
    spec['localPath_html'] = spec['localPath_html'].str.replace('data_lab/10k_sample3/data_html/',
                                                            'data_repo/10k_sample/data_html/')
    spec['parse_result'] = ap(spec, parallel_pcw)
    spec.to_csv('data_repo/10k_sample/spec2.csv', index = False)
    print('DONE')


def get_and_write_html(original_file_path, new_file_path):
    contents = read_html(original_file_path)
    html = re.search(r'<html>.+?</html>', contents, re.DOTALL | re.I)
    dirname = '/'.join(new_file_path.split('/')[0:-1])
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    if html:
        html = html.group()
        write_to_file(html, new_file_path)
        print('HTML found for ', original_file_path)
        print('HTML written to ', new_file_path)
        print()
        return True
    else:
        print('HTML not found for ', original_file_path)
        print()
        return False


def parallel_pcw2(dfrow):
    original_file_path = dfrow['original_file_path']
    new_file_path = dfrow['new_file_path']
    new_file_name = dfrow['new_file_name']
    html_file_dir_path = dfrow['html_file_dir_path']
    html_parsed_dir_path = dfrow['html_parsed_dir_path']

    try:
        find_html_check = get_and_write_html(original_file_path, new_file_path)
        if not find_html_check:
            return False

        parse_html_check = parse_clean_write(new_file_name, html_file_dir_path, html_parsed_dir_path)
        if not parse_html_check:
            return False
    except:
        print('an unknown error occured while processing the following file: ', original_file_path.split('/')[-1])
        print()


def main2():
    original_file_dir_path = 'data_repo/wu_sec_filing_data/raw_text_10k/'
    html_file_dir_path = 'data_repo/wu_sec_filing_data/html_text_10k/'
    html_parsed_dir_path = 'data_repo/wu_sec_filing_data/html_text_10k_parsed/'

    file_paths = get_all_txt_filepaths(original_file_dir_path)
    new_file_names = [x.split('/')[-1].replace('.txt', '.htm') for x in file_paths]
    new_file_paths = [html_file_dir_path + x for x in new_file_names]

    combos = list(zip(file_paths, new_file_paths, new_file_names,
                      [html_file_dir_path]*len(file_paths), [html_parsed_dir_path]*len(file_paths)))

    combos = pd.DataFrame.from_records(combos, columns = ['original_file_path', 'new_file_path', 'new_file_name',
                                                          'html_file_dir_path', 'html_parsed_dir_path'])
    with ProgressBar():
        combos['parse_result'] = ap(combos, parallel_pcw2)
    combos.to_csv('data_repo/wu_sec_filing_data/parse_results.csv', index=False)

main2()
