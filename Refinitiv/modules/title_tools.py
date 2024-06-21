import pandas as pd
import re
from fuzzywuzzy import process

direc = '/project/kh_mercury_1/conference_call/xml_pulls/codebase/'
name_dict = pd.read_csv(direc+'known_name_probs.csv', index_col=0).squeeze('columns').to_dict()


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
    # Use the same abbreviated firm suffix (e.g. incorporated to inc, corporation to corp, limited to ltd)
    name = re.sub('INCORPORATED', 'INC', name)
    name = re.sub('LIMITED PARTNERSHIP', 'LP', name)
    name = re.sub('CORPORATION', 'CORP', name)
    name = re.sub('LIMITED', 'LTD', name)
    name = re.sub('COMPANY', 'CO', name)
    name = re.sub('AKTIENGESELLSCHAFT', 'AG', name)
    name = re.sub('INTERNATIONAL', 'INTL', name)
    name = re.sub('\sINTERN', ' INTL', name)
    name = re.sub('\sINDUSTRIES', '', name)
    name = re.sub('P L C', 'PLC', name)
    name = re.sub('P.L.C', 'PLC', name)
    name = re.sub('L.P', 'LP', name)
    name = re.sub('\sL P', 'LP', name)
    name = re.sub('S.P.A', 'SPA', name)
    name = re.sub('S P A', 'SPA', name)
    name = re.sub('\sSYSTEM\s', ' SYS ', name)

    # Remove everything after firm name written in (...)
    name = re.sub(' \([A-Z].*', '', name)

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
    name = re.sub('\*', ' ', name)
    name = re.sub('Q[1-4]\s', '', name)
    name = re.sub('\sTHE\s|THE\s', '', name)
    name = re.sub('\.COM', '', name)
    name = re.sub('(FINAL\s|PRELIM|INTERIM\s|FULL\sYEAR|HALF\sYEAR|FY\s).*?[0-9]{4}\s', '', name)
    name = re.sub('(\sFINAL|\sPRELIM).*?RESULT', '', name)
    name = re.sub('[0-9]{4}(.*?)(?=[A-Z])', '', name)
    name = re.sub('\s-\s', '', name)
    name = re.sub('\sEARNINGS\s(.*)', '', name)
    name = re.sub('\sAT\s(.*)', '', name)
    name = re.sub('\sTO\s(.*)', '', name)
    name = re.sub('\sINVESTOR(.*)', '', name)
    name = re.sub('\sKEY PERFORMANCE(.*)', '', name)
    name = re.sub('\sCONFR(.*)', '', name)
    name = re.sub('\sCONFER(.*)', '', name)
    name = re.sub('\sANALYST(.*)', '', name)
    name = re.sub('\sMERGER\s(.*)', '', name)
    name = re.sub('\sVIDEO\s(.*)', '', name)
    name = re.sub('\sMANAG(.*)', '', name)
    name = re.sub('\sANNUAL\s(.*)', '', name)
    name = re.sub('\sFIRESIDE\s(.*)', '', name)
    name = re.sub('\sMIDTERM\s(.*)', '', name)
    name = re.sub('\sANNOUNCE(.*)', '', name)
    name = re.sub('\sDISCUSS(.*)', '', name)
    name = re.sub('\sCORPORATE\sS(.*)', '', name)
    name = re.sub('\sHOSTS\sS(.*)', '', name)
    name = re.sub('\s\'RESCHED(.*)', '', name)
    name = re.sub('\sAND\s[0-9]{4}\s(.*)', '', name)
    name = re.sub('\sAND\sYEAR-END{4}\s(.*)', '', name)
    name = re.sub('\sAUA\sAND\s(.*)', '', name)

    return name


#very strict cleaner to maximize chance of matching
def superclean(name):
    '''
    name: string of a firm name
    Returns bare minimum string to represent the company. 
    This is done by removing common suffixes (LLC, SPA, CO, SA, etc.), 
    as well as some additional cleans encountered through testing
    '''
    name = pdf_clean_title(name)
    #use name discrepancy info to minimize problems
    name = name_dict.get(name, name)
    #Remove non-standard things such as periods,commas,slashes,etc.
    name = re.sub('[^\-a-zA-Z0-9_\s]', '', name)

    # Remove common firm suffixes
    name = re.sub(' LLC\Z', '', name)
    name = re.sub(' SPA\Z', '', name)
    name = re.sub(' INC\Z', '', name)
    name = re.sub(' CO\Z', '', name)
    name = re.sub(' SA\Z', '', name)
    name = re.sub(' AB\Z', '', name)
    name = re.sub(' PLC\Z', '', name)
    name = re.sub(' CORP\Z', '', name)
    name = re.sub(' LTD\Z', '', name)
    name = re.sub(' AG\Z', '', name)
    name = re.sub(' ASA\Z', '', name)
    name = re.sub(' AS\Z', '', name)
    name = re.sub(' LP\Z', '', name)
    name = re.sub(' NV\Z', '', name)
    name = re.sub(' INTERNATIONAL\Z', '', name)
    name = re.sub(' INTL\Z', '', name)
    name = re.sub(' GROUP\Z', '', name)
    name = re.sub(' GP\Z', '', name)
    name = re.sub(' CAPITAL\Z', '', name)
    name = re.sub(' CAP\Z', '', name)
    name = re.sub(' REALTY\Z', '', name)
    name = re.sub(' RLTY\Z', '', name)
    name = re.sub(' AND\Z', '', name)
    
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
    nam = re.sub('^\*.*?(?=[A-Z])', '', nam)
    nam = re.sub('\.[A-Z][^\.](.*)', '', nam)
    nam = re.sub('\.[A-Z]\Z', '', nam)
    nam = re.sub('^\d+$', '', nam)
    nam = re.sub('^[A-Z][0-9]{6}', '', nam)

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
