How to dockerize sec mirror
===

## SEC-XML Site
 - Used to find all company information

## SEC-


## CIK Sources
CIK_LIST_CSV = '{0}/investment_company_series_class.csv'.format(CONFIG_RES_DIR)
CIK_LIST_PRASANNA = '{0}/cik.coleft.c.txt'.format(CONFIG_RES_DIR)

## sec_datafabric.py
takes CIK list from sources above
retrieves all filings in xmldict form for the CIK
Need to savve all this to database, lets go postgres or mongo
Need to get CIK list from somewhere else

## database
mongo or postgres
contains a mirror of all filing records, and dates updated, or downloaded

## filing downloaders
slave containers for downloading the content

## filing browser GUI
web application to manage scrapes, progress, look at database
