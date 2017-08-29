import html
import re
import unicodedata
import lxml

chars = {
        '\xc2\x82': ',',  # High code comma
        '\xc2\x84': ',,',  # High code double comma
        '\xc2\x85': '...',  # Tripple dot
        '\xc2\x88': '^',  # High carat
        '\xc2\x91': '\x27',  # Forward single quote
        '\xc2\x92': '\x27',  # Reverse single quote
        '\xc2\x93': '\x22',  # Forward double quote
        '\xc2\x94': '\x22',  # Reverse double quote
        '\xc2\x95': ' ',
        '\xc2\x96': '-',  # High hyphen
        '\xc2\x97': '--',  # Double hyphen
        '\xc2\x99': ' ',
        '\xc2\xa0': ' ',
        '\xc2\xa6': '|',  # Split vertical bar
        '\xc2\xab': '<<',  # Double less than
        '\xc2\xbb': '>>',  # Double greater than
        '\xc2\xbc': '1/4',  # one quarter
        '\xc2\xbd': '1/2',  # one half
        '\xc2\xbe': '3/4',  # three quarters
        '\xca\xbf': '\x27',  # c-single quote
        '\xcc\xa8': '',  # modifier - under curve
        '\xcc\xb1': ''  # modifier - under line
    }

def replace_chars(match):
    char = match.group(0)
    return chars[char]

def clean_anomalies(text_str):
    """
        Clean various anomalies resulting from formatting and unicode

        Args:
            text_str (string): 
                string to clean
        Returns:
            text_str (string): 
                cleaned string
    """
    text_str = re.sub('(' + '|'.join(chars.keys()) + ')', replace_chars, text_str)
    text_str = re.sub('(-[\n\r\t\v])', '-',
                      text_str)  # replace dashes followed by a newline or tab with a dash
    text_str = re.sub('(&[\S]{2,6};)', '', text_str)  # remove all remaining html chars starting with &
    text_str = re.sub('(#[a-f0-9]{6})', '', text_str)  # remove all remaining html chars starting with #
    text_str = re.sub('(\s-\s)', ' ', text_str)
    text_str = re.sub('and/or', 'and or', text_str, flags=re.IGNORECASE)
    text_str = re.sub('(([-.=]){2,}\s*)', ' ', text_str)
    text_str = re.sub('_', '', text_str)
    text_str = re.sub('(\s){3,}', ' ', text_str)
    text_str = re.sub('(\n\s*?){3,}', '\n\n', text_str)
    text_str = re.sub('(?<![\n])(\n)(?![\n\s])', ' ', text_str)
    return text_str


def preprocess_html(text_str, plain_text=True, remove_newlines=True, remove_tabs=True, remove_tables=False):
    """
        Pre-process html string by cleaning anomalies and optionally removing newlines, tabs, and html tables

        Args:
            text_str (string): 
                html string to preprocess
            plain_text (bool, optional, default True): 
                if True, remove all html markup and return only the text
            remove_newlines (bool, optional, default True): 
                if True, remove all newlines
            remove_tabs (bool, optional, default True): 
                if True, remove all tabs
            remove_tables (bool, optional, default False): 
                if True, remove all html tagged tables

        Returns:
            text_str (string): 
                pre-processed html string
    """
    text_str = html.unescape(unicodedata.normalize('NFKD', text_str))
    text_str = re.sub('[\r]', '\n', text_str)
    if remove_tables:
        text_str = re.sub('(<table>.+?</table>)', '\n', text_str, flags=re.I)
    if plain_text:
        text_str = clean_anomalies(lxml.html.fromstring(text_str).text_content())
    else:
        text_str = text_str = clean_anomalies(text_str)
    if remove_newlines:
        text_str = re.sub('[\n]+', ' ', text_str)  # remove newlines
    if remove_tabs:
        text_str = re.sub('[\t]+', ' ', text_str)  # remove tabs
    return text_str