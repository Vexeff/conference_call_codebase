from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
import os
import datetime

local_remotes = '/Users/erouhani/Desktop/Workspace/Refinitiv/Remote_ref'


def get_xml_body(ref_con, path, db=None):
    '''
    ref_con: sftp connection or None
    path: path to the xml within mercury 
    db: refinitiv or factset -- needs to be entered as 'ref' or 'fs'
    Returns transcript of call 
    '''
    if ref_con:
        f = ref_con.open(path)
    else:
        f = open(path)
    if db not in ['fs', 'ref']:
        raise ValueError('invalid db: '+db+' -- Given db needs to be "fs" or "ref"')
    data = bs(f, 'xml')
    if db == 'ref':
        transcript = data.Body.text
    else:
        transcript = data.body.text
    f.close()
    return transcript


#gets xml meta info for pull comparison
def get_xml_metas(ref_con, path):
    '''
    ref_con: sftp connection or None
    path: path to the xml within mercury 
    Returns cusip of company if there is one, firm name, 
    event title, and date of event
    '''
    if ref_con:
        f = ref_con.open(path)
    else:
        f = open(path)
    data = bs(f, 'xml')
    act = data.EventStory.get('action')
    if act == 'delete':
        f.close()
        return False
    date = pd.to_datetime(data.startDate.contents[0])
    firm_id = np.nan
    try:
        firm_id = data.CUSIP.contents
    except:
        print(f'{path} faced an error for data.CUSIP.contents')
        firm_id = np.nan
    try:
        firm_id = firm_id[0]
    except:
        firm_id = np.nan
    firm_name = data.companyName.contents[0]
    title = data.eventTitle.contents[0]
    f.close()
    return [firm_id, firm_name, title, date]

#week-of-month buffer (defined as a week index assigned to day+-3)
def get_buffer(date):
    '''
    date: date data type
    Returns 7 day period including the given day.
    In other words, returns day +- 3.
    Checks of edge cases for months.
    '''
    day = date.day
    dim = date.daysinmonth
    
    lower = day-3
    upper = day+3
    
    if lower < 1:
        extra_days = np.abs(lower)
        prev_dim = (date-datetime.timedelta(days=28)).daysinmonth
        lower_padding = np.arange(prev_dim-extra_days, prev_dim+1)
        return np.append(np.arange(0, upper+1), lower_padding)
    if upper > dim:
        extra_days = upper-dim
        upper_padding = np.arange(1, extra_days+1)
        return np.append(np.arange(lower, dim+1), upper_padding)
    return np.arange(lower, upper+1)

#date-sub extractor
def date_subtype(date, subtype):
    '''
    date: date data type
    subtype: type of date subtype i.e.: year, month, buffer
    Returns relevant subtype. 
    Checks for edge cases and returns list if day is less than 
    a week from being in a different subtype.
    '''
    if subtype != 'buffer':
        sub = getattr(date, subtype)
    else:
        return get_buffer(date)

    day = date.day
    month = date.month
    #check if day is on edge
    if day < 7 or day > 24:
        match subtype:
            case 'year':
                if day < 7:
                    if month == 1:
                        return [sub, sub-1]
                else:
                    if month == 12:
                        return [sub, sub+1]
            case 'month':
                if day < 7:
                    if sub == 1:
                        return [sub, 12]
                    return [sub, sub-1]
                else:
                    if sub == 12:
                        return [sub, 1]
                    return [sub, sub+1]
    return sub