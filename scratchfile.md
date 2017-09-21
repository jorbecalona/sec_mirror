# scratchfile.md
xmldict['feed']['author'] == OrderedDict(
    [('email', 'webmaster@sec.gov'), ('name', 'Webmaster')])
    xmldict['feed']['author']['email'] == 'webmaster@sec.gov'
    xmldict['feed']['author']['name'] == 'Webmaster'
xmldict['feed']['title'] == 'OPPENHEIMER PRINCIPAL PROTECTED TRUST II  (0001260507)'
xmldict['feed']['updated'] == '2017-08-29T09:47:06-04:00'
xmldict['feed']['id'] == 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507'
xmldict['feed']['company-info'] == ['addresses', 'cik', 'cik-href',
                                    'conformed-name', 'fiscal-year-end', 'state-location', 'state-location-href']
    xmldict['feed']['company-info']['addresses']
        xmldict['feed']['company-info']['addresses']['address'] == [
            odict_keys(['@type', 'city', 'phone', 'state', 'street1', 'zip'])]
    xmldict['feed']['company-info']['cik'] == '0001260507'
    xmldict['feed']['company-info']['cik-href'] == 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&owner=exclude&count=100'
    xmldict['feed']['company-info']['conformed-name'] == 'OPPENHEIMER PRINCIPAL PROTECTED TRUST II'
    xmldict['feed']['company-info']['fiscal-year-end'] == '0331'
    xmldict['feed']['company-info']['state-location'] == 'CO'
    xmldict['feed']['company-info']['state-location-href'] == 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&State=CO&owner=exclude&count=100'
xmldict['feed']['link']
    xmldict['feed']['link']['@href'] == 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001260507&type=&datea=&dateb=&owner=exclude&count=100&output=atom&start=100'
    xmldict['feed']['link']['@rel'] == 'next' 'alternate' 'self'
    xmldict['feed']['link']['@type'] == 'application/atom+xml'
xmldict['feed']['entry'] == [['category', 'content',
                              'id', 'link', 'summary', 'title', 'updated']]
    xmldict['feed']['entry']['category'] == ['@label', '@scheme', '@term']
    xmldict['feed']['entry']['content'] == ['@type', 'accession-nunber', 'act', 'file-number',
                                            'file-number-href', 'filing-date', 'filing-href', 'filing-type', 'film-number', 'form-name', 'size']
    xmldict['feed']['entry']['id'] == 'urn:tag:sec.gov,2008:accession-number=9999999997-12-008331'
    xmldict['feed']['entry']['link'] == ['@href', '@rel', '@type']
    xmldict['feed']['entry']['summary'] == ['@type', '#text'
    xmldict['feed']['entry']['title'] == 'N-8F ORDR  - N-8F Order''
    xmldict['feed']['entry']['updated'] == '2012-04-24T13:50:38-04:00'

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
