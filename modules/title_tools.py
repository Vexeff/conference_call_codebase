import pandas as pd
import re
from thefuzz import process

# local laod
# direc = './'
# remote load
direc = '/project/kh_mercury_1/conference_call/ciq/'
name_dict = pd.read_csv(direc+'data_ref/known_name_probs.csv', index_col=0).squeeze('columns').to_dict()


def clean_ref_body(txt):
    '''
    This functions cleans refinitiv call transcripts
    Input: extracted call body from refinitiv
    Output: Same text cleaned as much as possible with paragraph breaks 
    added appropriately
    '''
    txt = re.sub(r'-{2,}', '', txt)
    txt = re.sub(r',{0,}\s*\[[0-9]{0,}\]((.|\n)*?)(?=([A-Z]|[a-z]))', ': ', txt)
    return txt


def standardize_suffix(title):
    '''
    Standardizes common firm suffixes
    '''
    title = title.upper()

    #Standardize common firm suffixes
    title = re.sub(r'\sSPÓLKA\sAKCYJNA', ' SA', title)
    title = re.sub(r'\sSOCIÉTÉ\sANONYME', ' SA', title)
    title = re.sub(r'\sSOCIEDAD\sANÓNIMA', ' SA', title)
    title = re.sub(r'\sANONIM\sSIRKETI', ' AS', title)
    title = re.sub(r'\sBERHAD', ' BHD', title)
    title = re.sub(r'\sAKTIENGESELLSCHAFT', ' AG', title)
    title = re.sub(r'\sAKTIEBOLAGET', ' AB', title)
    title = re.sub(r'\sAKTIEBOLAG', ' AB', title)
    title = re.sub(r'\sNAAMLOZE\sVENNOOTSCHAP', ' NV', title)
    title = re.sub(r'\sPUBLIC\sLIMITED\sCOMPANY', ' PLC', title)
    title = re.sub(r'\sOYJ', ' PLC', title)
    title = re.sub(r'\sPUBLIC\sCOMPANY\sLIMITED', ' PCL', title)
    title = re.sub(r'\sPUBLIC\sCO\sLTD', ' PCL', title)
    title = re.sub(r'\sJOINT\sSTOCK\sCOMPANY', ' JSC', title)
    title = re.sub(r'\sJOINT\sSTOCK\sCO', ' JSC', title)
    title = re.sub(r'\sREAL\sESTATE\sINVESTMENT\sTRUST', ' REIT', title)
    title = re.sub(r'\sINTERNATIONAL', ' INTL', title)
    title = re.sub(r'\sS\sA(?:\s+|$)', ' SA', title)
    title = re.sub(r'\sA\sB(?:\s+|$)', ' AB', title)
    title = re.sub(r'\sA\sG(?:\s+|$)', ' AG', title)
    title = re.sub(r'\sA\sS(?:\s+|$)', ' AS', title)
    title = re.sub(r'\sL\sP(?:\s+|$)', ' LP', title)
    title = re.sub(r'\sL\.P(?:\s+|$)', ' LP', title)
    title = re.sub(r'\sN\sV(?:\s+|$)', ' NV', title)
    title = re.sub(r'\sC\sV(?:\s+|$)', ' CV', title)
    title = re.sub(r'\sA\sS\sA(?:\s+|$)', ' ASA', title)
    title = re.sub(r'\sS\sA\sB(?:\s+|$)', ' SAB', title)
    title = re.sub(r'\sP\sL\sC(?:\s+|$)', ' PLC', title)
    title = re.sub(r'\sP\sC\sL(?:\s+|$)', ' PCL', title)
    title = re.sub(r'\sB\sH\sD(?:\s+|$)', ' BHD', title)
    title = re.sub(r'\sO\sY\sJ(?:\s+|$)', ' OYJ', title)
    title = re.sub(r'\sINCORPORATED', ' INC', title)
    title = re.sub(r'\sCOMPANY', ' CO', title)
    title = re.sub(r'\sCORPORATION', ' CORP', title)
    title = re.sub(r'\sLIMITED', ' LTD', title)
    title = re.sub(r'\sINTERNATIONAL', ' INTL', title)
    title = re.sub(r'\sGROUP', ' GP', title)
    title = re.sub(r'\sGROEP', ' GP', title)
    title = re.sub(r'\sCAPITAL', ' CAP', title)
    title = re.sub(r'\sREALTY', ' RLTY', title)

    return title

    

#cleaner function for pdf-based title cleaning
def pdf_clean_title(title):
    '''
    ***
    This function is designed to clean titles of refinitiv PDFs.
    ***
    title: string of PDF title
    Returns cleaned title, which should be the firm name
    '''
    name = title.upper()
    
    # standardize firm suffix names
    name = standardize_suffix(name)

    # Remove everything after firm name written in (...)
    name = re.sub(r' \([A-Z].*', '', name)

    # Remove everything before firm name if it is "EVENT TRANSCRIPT OF"  or "EVENT BRIEF OF"
    name = re.sub('^.+EVENT TRANSCRIPT OF ', '', name)
    name = re.sub('^.+EVENT BRIEF OF ', '', name)

    # Remove everything after "CONFERENCE"
    name = re.sub(' CONF.+$', '', name)

    # Remove everything after firm name if it is " - PRELIM..." or " _ FINAL..."
    name = re.sub(' - +.*', '', name)

    return name

#cleaner function for xml-based title cleaning
def xml_clean_title(title):
    '''
    ***
    This function is designed to clean event titles of refinitiv XMLs.
    ***
    title: string of PDF title
    Returns cleaned title, which should be a firm name
    '''
    name = title.upper()
    name = re.sub(r'\*', ' ', name)
    name = re.sub(r'Q[1-4]\s', '', name)
    name = re.sub(r'\sTHE\s|THE\s', '', name)
    name = re.sub(r'\.COM', '', name)
    name = re.sub(r'(FINAL\s|PRELIM|INTERIM\s|FULL\sYEAR|HALF\sYEAR|FY\s).*?[0-9]{4}\s', '', name)
    name = re.sub(r'(\sFINAL|\sPRELIM).*?RESULT', '', name)
    name = re.sub(r'[0-9]{4}(.*?)(?=[A-Z])', '', name)
    name = re.sub(r'\s-\s', '', name)
    name = re.sub(r'\sEARNINGS\s(.*)', '', name)
    name = re.sub(r'\sAT\s(.*)', '', name)
    name = re.sub(r'\sTO\s(.*)', '', name)
    name = re.sub(r'\sINVESTOR(.*)', '', name)
    name = re.sub(r'\sKEY PERFORMANCE(.*)', '', name)
    name = re.sub(r'\sCONFR(.*)', '', name)
    name = re.sub(r'\sCONFER(.*)', '', name)
    name = re.sub(r'\sANALYST(.*)', '', name)
    name = re.sub(r'\sMERGER\s(.*)', '', name)
    name = re.sub(r'\sVIDEO\s(.*)', '', name)
    name = re.sub(r'\sMANAG(.*)', '', name)
    name = re.sub(r'\sANNUAL\s(.*)', '', name)
    name = re.sub(r'\sFIRESIDE\s(.*)', '', name)
    name = re.sub(r'\sMIDTERM\s(.*)', '', name)
    name = re.sub(r'\sANNOUNCE(.*)', '', name)
    name = re.sub(r'\sDISCUSS(.*)', '', name)
    name = re.sub(r'\sCORPORATE\sS(.*)', '', name)
    name = re.sub(r'\sHOSTS\sS(.*)', '', name)
    name = re.sub(r'\s\'RESCHED(.*)', '', name)
    name = re.sub(r'\sAND\s[0-9]{4}\s(.*)', '', name)
    name = re.sub(r'\sAND\sYEAR-END{4}\s(.*)', '', name)
    name = re.sub(r'\sAUA\sAND\s(.*)', '', name)

    # standardize firm suffix names
    name = standardize_suffix(name)

    #Remove any extra remaining whitespace at the beginning or end of the string
    name = re.sub(r'^\s*|\s*$', '', name)

    return name

#simple cleaning function that standardizes firm suffixes and removes punctuation
def ciq_clean_title(title):
    '''
    Runs a simple clean of company titles such as removing non-standard
    characters, standardizing company suffixes, etc. 
    Note: Somewhat specific to refinitiv xml titles, pdf titles, 
    Compustat gvkey/title dataset, and WRDS Sec Historical title dataset
    '''
    title = title.upper()

    # Removing 'Q{1-4} Earnings' etc.
    title = re.sub('Q[1-4] [0-9]{1,4}.*$', '', title)
    # Removing 'H{1-2} Earnings' etc.
    title = re.sub('H[1-2] [0-9]{1,4}.*$', '', title)
    # Removing {2020-2023} Earnings
    title = re.sub('[0-9]{1,4} EARN.*$', '', title)
    # Removing {2020-2023} Pre-Recorded Earnings
    title = re.sub('[0-9]{1,4} PRE REC.*$', '', title)
    # Remove 'Presents at' etc.
    title = re.sub('PRESENTS AT.*$', '', title)
    # Remove '... - Shareholder/...' etc.
    title = re.sub('- SHAREHOLDER/.*$', '', title)
    # Remove '... - Analyst/...' etc.
    title = re.sub('- ANALYST/.*$', '', title)
    # Remove '... - M&A ...' etc.
    title = re.sub('- M&A.*$', '', title)
    # Remove '... - Special ...' etc.
    title = re.sub('- SPECIAL.*$', '', title)

    # Remove non-standard things such as periods,commas,slashes,etc.
    # Add space after commas before removing them
    title = re.sub(',(?=[A-Z1-9])', ' ', title)
    title = re.sub(r'\(PUBL?.\)', ' ', title)
    title = re.sub("[(),.'/]", '', title)
    if r'\\' in repr(title):
        title = re.sub('\\\\*', '', repr(title))
        title = eval(title)
    title = re.sub(r'\s\-\s', ' ', title)
    title = re.sub(r'\-', ' ', title)
    title = re.sub(r'\s&\s', ' AND ', title)

    # standardize firm suffix names
    title = standardize_suffix(title)

    #Remove any extra remaining whitespace at the beginning or end of the string
    title = re.sub(r'^\s*|\s*$', '', title)

    return title.upper()


#very strict cleaner to maximize chance of matching
def superclean(name):
    '''
    name: string of a firm name \n
    db: string determining if ciq clean title should be used instead of \
    traditional refinitiv-based cleaner\n

    Returns bare minimum string to represent the company. 
    This is done by removing common suffixes (LLC, SPA, CO, SA, etc.), 
    as well as some additional cleans encountered through testing
    '''
    name = name.upper()
    # standardize suffixes in firm names
    name = standardize_suffix(name)

    #use name discrepancy info to minimize problems
    name = name_dict.get(name, name)

    #Remove non-standard things such as periods,commas,slashes,etc.
    name = re.sub(r'[^\-a-zA-Z0-9_\s]', '', name)

    # remove leading 'the' due to cross-db mismatches
    name = re.sub('^THE', '', name)

    # Remove common firm suffixes
    name = re.sub(r' LLC\Z', '', name)
    name = re.sub(r' SPA\Z', '', name)
    name = re.sub(r' INC\Z', '', name)
    name = re.sub(r' CO\Z', '', name)
    name = re.sub(r' SA\Z', '', name)
    name = re.sub(r' AS\Z', '', name)
    name = re.sub(r' AB\Z', '', name)
    name = re.sub(r' PLC\Z', '', name)
    name = re.sub(r' PCL\Z', '', name)
    name = re.sub(r' CORP\Z', '', name)
    name = re.sub(r' LTD\Z', '', name)
    name = re.sub(r' OYJ\Z', '', name)
    name = re.sub(r' BHD\Z', '', name)
    name = re.sub(r' AG\Z', '', name)
    name = re.sub(r' ASA\Z', '', name)
    name = re.sub(r' SAB\Z', '', name)
    name = re.sub(r' LP\Z', '', name)
    name = re.sub(r' NV\Z', '', name)
    name = re.sub(r' JSC\Z', '', name)
    name = re.sub(r' REIT\Z', '', name)
    name = re.sub(r' LP\Z', '', name)
    name = re.sub(r' CV\Z', '', name)
    name = re.sub(r' INTL\Z', '', name)
    name = re.sub(r' GROUP\Z', '', name)
    name = re.sub(r' GP\Z', '', name)
    name = re.sub(r' CAPITAL\Z', '', name)
    name = re.sub(r' CAP\Z', '', name)
    name = re.sub(r' REALTY\Z', '', name)
    name = re.sub(r' RLTY\Z', '', name)
    name = re.sub(r' AND\Z', '', name)

    # Remove everything after firm name written in (...)
    name = re.sub(r' \([A-Z].*', '', name)

    #Remove any extra remaining whitespace at the beginning or end of the string
    name = re.sub(r'^\s*|\s*$', '', name)
    
    return name

#cleaner for tickers
def clean_tickers(title):
    '''
    This function helps clean event_titles (subtitles) for PDFs 
    when the cleaner returns tickers instead of names. 
    title: event title after being cleaned
    Returns a substringed version of event title that makes tickers
    more obvious
    '''
    nam = title.upper()
    nam = re.sub(r'^\*.*?(?=[A-Z])', '', nam)
    nam = re.sub(r'\.[A-Z][^\.](.*)', '', nam)
    nam = re.sub(r'\.[A-Z]\Z', '', nam)
    nam = re.sub(r'^\d+$', '', nam)
    nam = re.sub(r'^[A-Z][0-9]{6}', '', nam)

    return nam

#basic compare function to make fuzzy matching more efficient
def basic_compare(str1, strs):
    '''
    str1: string of firm name to be matched
    strs: **unique** list of possible string matches
    returns a supercleaned version of str1 as well as a dataframe of 
    given strs with their cleaned counterparts

    Removes some string options by string length rules to make
    next comparison steps more efficient
    '''
    str1 = superclean(str1)
    str1_nospace = str1.replace(' ', '')
    str1_len = len(str1_nospace)
    strs = pd.DataFrame(strs, columns=['names'])
    strs['cleaned'] = strs.names.apply(superclean)
    strs['nospace'] = strs.cleaned.str.replace(' ', '')
    strs['lens'] = strs.nospace.apply(len)
    strs = strs[((str1_len -2) <= strs.lens) & (strs.lens <= (str1_len+2))]
    strs.set_index('cleaned', inplace=True)
    return [str1, strs[['names']]]

#fuzzy matching function
def compare_names(str1, strs):
    '''
    str1: string of firm name to match
    strs: **unique** list of possible string matches
    Returns True if fuzzy match had a >= 95% match, as well as
    the best match from strs
    '''
    basic_res = basic_compare(str1, strs)
    str1 = basic_res[0]  
    strs_df = basic_res[1]
    if strs_df.shape[0] < 1:
        return [False]
    best = process.extractOne(str1, list(strs_df.index))
    rating = True if best[1] >= 95 else False
    final = strs_df.loc[best[0]]
    res = final.values[0]
    if final.shape[0] > 1:
        res = final['names'].iloc[0]
        
    return [rating, res]