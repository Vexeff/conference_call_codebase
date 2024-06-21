import os
import pandas as pd
import numpy as np
import paramiko as pk
import modules.xml_mods as xml_tools
import modules.title_tools as t_tools
import time
import re
import datetime

univ = '/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive'
master = pd.read_csv('/project/kh_mercury_1/conference_call/xml_pulls/data_ref/Refinitiv_historical_web.csv', low_memory=False, index_col=0)

def run_year(running_dups, yr):
    '''
    NOTE: MEANT FOR HISTORICAL REFINITIV CHECKING\n
    running_dups: running list of duplicates found while running
    yr: folder year to run on

    Returns updated running_dups list, as well as a dataframe for the year. 
    
    Each row in df is an xml file in the folder, columns are:
    duplicate: Boolean -- whether file already existed in master csv
    folder_year: Given folder year
    file_name: unique transcript ID assigned by associated database
    firm_id: cusip of firm, if it exists
    firm_name: name of firm
    event_title: title of event
    event_date: date of event
    if matched to a file in our refinitiv PDF database, following columns are not NA:
    ref_ReportID: report id of associated PDF
    ref_title: title of associated PDF
    event_title: event subtitle of associated PDF 
    ref_firmname: firm name of company in associated PDF
    '''
    os.chdir(univ)
    prev_year = yr-1
    next_year = yr+1
    loc_master = master[((master.Year == prev_year) & (master.Month == 12)) | ((master.Year == next_year) & (master.Month == 1)) | (master.Year == yr)]
    total = 0
    dups = 0
    new = 0
    govcount = 0

    files_to_get = []
    start_time = time.time()
    file_list = os.listdir(str(yr))

    for fil in file_list:
        #count total files looked through as you're looping
        total = total + 1
        #if file is already counted as duplicate, skip it
        if fil in running_dups:
            print(f'{fil} repeated')
            dups += 1
            continue
        metas = xml_tools.get_xml_metas(None,f'{yr}/'+fil)
        if not metas:
            continue
        firm_name = metas[1]
        title = metas[2]
        if firm_name in 'United States of America (Government)' or 'Government' in firm_name:
            govcount += 1
            continue
        title_name = t_tools.xml_clean_title(title)
        date = metas[3]
        #get date-info
        Year = xml_tools.date_subtype(date, 'year')
        Month = xml_tools.date_subtype(date, 'month')
        Day_Buffer = xml_tools.date_subtype(date, 'buffer')
        #create dictionary that helps with subsetting
        time_checks = {'Year': Year, 'Month': Month, 'Day_Buffer': Day_Buffer}
        #get subspace of master dataset 
        subspace = loc_master
        size = 10
        check_tick = 0
        for check_key in time_checks:
            check_val = time_checks[check_key]
            if check_tick > 3 or size < 2:
                break
            if isinstance(check_val, list):
                subspace = subspace[(subspace[f'{check_key}'] == check_val[0]) | (subspace[f'{check_key}'] == check_val[1])]
            elif isinstance(check_val, np.ndarray):
                subspace = subspace[subspace.Day.isin(check_val)]
            else:
                subspace = subspace[subspace[f'{check_key}'] == check_val]
            check_tick = check_tick + 1
            size = subspace.shape[0]
        matched = False
        match_ref = [np.nan, np.nan, np.nan]
        name_check = [False, False]
        if size > 0:
            while not matched:
                #try title checking with firm_name and title based name
                match_set = subspace[subspace.event_title.str.upper().str.contains(re.escape(t_tools.superclean(firm_name))) | subspace.Title.str.upper().str.contains(re.escape(t_tools.superclean(firm_name)))]
                if match_set.shape[0] > 0:
                    matched = True
                    break
                match_set = subspace[subspace.event_title.str.upper().str.contains(re.escape(t_tools.superclean(title_name))) | subspace.Title.str.upper().str.contains(re.escape(t_tools.superclean(title_name)))]
                if match_set.shape[0] > 0:
                    matched = True
                    break
                #start fuzzy matching
                #find unique set of possible names
                firm_opts = set(subspace.firm_name)
                #compare_names runs fuzzy matching on firm_names and returns best firm name match, if it finds one with sufficient similarity
                name_check = t_tools.compare_names(firm_name, firm_opts)
                #if match is found with standard method:
                if name_check[0]:
                    matched = True
                    break
                #try with title name
                name_check = t_tools.compare_names(title_name, firm_opts)
                if name_check[0]:
                    matched = True
                    break
                break
        if matched:
            #file is duplicated
            #if match was done by fuzzy matching, need to get match_set
            if name_check[0]:
                match_set = subspace[subspace.firm_name == name_check[1]]
            dups += 1
            #add file to running list of duplicate files
            running_dups.append(fil)
            #save original dataset references for recordkeeping
            match_ref = match_set[['ReportID', 'Title', 'event_title', 'firm_name']].sample(1).values[0].tolist()
        else:
            #file is new
            new += 1
        #Add file info to the dataset with a boolean to indicate it's new
        metas.insert(0, fil)
        metas.insert(0, yr)
        #this is the boolean for if there was a match
        metas.insert(0, matched)
        metas.extend(match_ref)
        files_to_get.append(metas)
        
    print(f'{yr} new rate: {round((new/(total-govcount))*100, 3)}%\n')
    print(f'{yr} match rate: {round((dups/(total-govcount))*100, 3)}%\n')
    print(f'Year {yr} took {(time.time() - start_time)} seconds')
    print(f'Processed {total} files. Found {dups} duplicates, and {new} new files.\n')
    return [running_dups, pd.DataFrame(files_to_get, columns=['duplicate', 'folder_year', 'file_name', 'firm_id', 'firm_name', 'event_title', 'event_date', 'ref_ReportID', 'ref_title', 'ref_event_title', 'ref_firm_name'])]
