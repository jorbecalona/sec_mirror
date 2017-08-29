from bs4 import BeautifulSoup
from bs4.element import NavigableString as bs4_navstring
from collections import OrderedDict
from difflib import SequenceMatcher as SM
import itertools
import re
from scrapy.selector import Selector
import string

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

def normalize_text(text_str, check_alphanum=True):
    """
    Normalize string by removing newlines, punctuation, spaces, and optionally filtering for alphanumeric chars

    Args:
        text_str (string): 
            string to normalize
        check_alphanum (bool, optional, default True): 
            if True, only return elt if it contains at least one alphanumeric char, return None otherwise

    Returns:
        text_str (string): 
            normalized string or None
    """
    text_str = text_str.replace('\n', '')  # remove new lines
    translator = str.maketrans('', '', string.punctuation)
    text_str = text_str.lower().translate(translator)  # lowercase then remove punctuation
    text_str = text_str.strip().replace(' ', '_')  # replace spaces with underscores
    if check_alphanum:
        alphanum_check = re.search('[a-zA-Z0-9]', text_str)
        if alphanum_check:
            return text_str
        else:
            return None
    else:
        return text_str

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

def parse_items(html_str, fuzzy_threshold=0.8, marked_html=False, max_num_missing_items=0):
    """
        Parse items from html string and return either a list of dicts or a modified html string with items marked

        Args:
            html_str (string): 
                html string from which to parse items
            fuzzy_threshold (float, optional, default 0.8): 
                minimum tolerable fuzzy similarity between the actual item title (above) and the one found
            marked_html (bool, optional, default False): 
                if True, return a modified html string with parsed items marked by <div class='marked_item' id=[item_label]
            max_num_missing_items (int, optional, default 0): 
                maximum tolerable quantity of missing items
            remove_tables (bool, optional, default True): 
                if True, remove all html tagged tables

        Returns:
            (OrderedDict): (if marked_html is False)
                dict with item labels as keys and the html text as values
            (string): (if marked_html is True)
                marked up html string with parsed items marked by <div class='marked_item' id=[item_label]
    """
    # 1. find table of contents rows in html string
    sel = Selector(text=html_str, type='html')
    table_row_path = '//table//tr[(td//text()[re:match(.,"item","i")]) and (td//a[contains(@href,"#")])]'
    toc_rows = sel.xpath(table_row_path)
    if not toc_rows:
        print('no links found')
        return False

    # 2. find text of rows and the first occuring link in each row (there should only be one unique link per row)
    toc_rows_text = [get_unique_elts(x.xpath('.//text()[re:match(.,"[a-zA-Z_]")]').extract()) for x in toc_rows]
    toc_rows_text = [list(map(lambda x: normalize_text(x, check_alphanum=True), temp_row)) for temp_row in toc_rows_text]
    toc_rows_text = [x for x in toc_rows_text if x] # get all elements that are not None
    toc_rows_links = [get_unique_elts(x.xpath('.//a/@href').extract())[0] for x in toc_rows]  #one link per row

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
        elif fuzzy_threshold < 1:  # perform fuzzy matching on item titles
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
    missing_items = list(set(item_labels) - set(toc_rows2_labels))
    if set(toc_rows2_labels) != set(item_labels):
        if len(missing_items) > max_num_missing_items:
            print('number of missing items is larger than threshold')
            print('the following items are missing: ', str(missing_items))
            return False
        else:
            print('some items are missing, but threshold number is not exceeded')
            print('the following items are missing: ', str(missing_items))

    if marked_html:
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
        return OrderedDict([(row_dict['label'], row_dict['html']) for row_dict in toc_rows2]), str(soup)
    else:
        end_ind = len(html_str)
        for row_dict in reversed(toc_rows2):
            link_match = re.search('<a[\s\n]?name="{0}"[^>]*?>'.format(row_dict['link'].replace('#', '')),
                                                    html_str, re.IGNORECASE)
            if link_match:
                start_ind = link_match.span()[0]
                row_dict.update({'html': html_str[start_ind:end_ind]})
            end_ind = start_ind

        return OrderedDict([(row_dict['label'], row_dict['html']) for row_dict in toc_rows2]), ''
