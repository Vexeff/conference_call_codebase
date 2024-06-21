import datetime
import pandas as pd
import numpy as np
import modules.xml_mods as xml_tools
import modules.title_tools as t_tools
import re
import ast
import os
import time

# change to overall xml_pulls directory
univ = '/project/kh_mercury_1/conference_call/xml_pulls/'
os.chdir(univ)

# define output directory
output_dir = './data_ref/get_files'

# pick file output name -- e.g. 2023_q3 will show up as '...gets_2023_q3'
output_name = '2024_q1'

# update to master dataset of factset transcripts
fs_master = pd.read_csv('./data_ref/fs_master_accessible.csv')
# filter to unprocessed (newly added) files
fs_xmls = fs_master[fs_master.is_duplicate.isna()]

#update to most recent ciq dataset
# NOTE: this process used to run on ref_master, which is no longer used
ciq_master = pd.read_csv('../ciq/data_ref/ciq_master.csv', dtype=str)
ciq_master['Year'] = pd.DatetimeIndex(ciq_master.event_date).year
ciq_master['Month'] = pd.DatetimeIndex(ciq_master.event_date).month
ciq_master['Day'] = pd.DatetimeIndex(ciq_master.event_date).day

# initialize dataframe of new factset transcripts
fs_new = pd.DataFrame()

# start timer
start_time = time.time()

# go through all new fs xmls and download new needed ones
for i, rest in fs_xmls.iterrows():
    # print progress at every 1000 rows
    if not fs_new.shape[0] % 1000:
        print(f'{round(time.time()-start_time)} seconds have elapsed.\n{fs_new.shape[0]} new files have been found.')
    
    # get report id
    report_id = rest.REPORT_ID
    # get file name
    file_name = rest.file_name
    # get firm cusip
    firm_id = rest.CUSIP
    # get firm name
    firm_name = rest.ENTITY_NAME
    # get title_name -- very minor difference in the case of factset
    title_name = rest.ENTITY_PROPER_NAME

    # get subspace of master dataset 
    subspace = ciq_master

    # ensure date type 
    date = pd.to_datetime(rest.EVENT_DATETIME_UTC)

    # set up date filtering process
    Year = xml_tools.date_subtype(date, 'year')
    Month = xml_tools.date_subtype(date, 'month')
    Day_Buffer = xml_tools.date_subtype(date, 'buffer')
    time_checks = {'Year': Year, 'Month': Month, 'Day_Buffer': Day_Buffer}

    # initialize values before data filtering
    size = 10
    check_tick = 0
    # start date filtering
    for check_key in time_checks:
        # get value of data filter
        check_val = time_checks[check_key]
        # exit conditions to leave comparison
        if check_tick > 3 or size < 2:
            break
        # apply date filter
        if isinstance(check_val, list):
            subspace = subspace[(subspace[f'{check_key}'] == check_val[0]) | (subspace[f'{check_key}'] == check_val[1])]
        elif isinstance(check_val, np.ndarray):
            subspace = subspace[subspace.Day.isin(check_val)]
        else:
            subspace = subspace[subspace[f'{check_key}'] == check_val]
        # count date filtering progress
        check_tick = check_tick + 1
        # save new subspace size
        size = subspace.shape[0]

    # initialize values before matching process
    matched = False
    match_ref = [np.nan, np.nan, np.nan]
    name_check = [False, False]
    # exit if subspace is too small
    if size > 0:
        while not matched:
            #try cusip matching if possible
            if isinstance(firm_id, str):
                cusips = ast.literal_eval(firm_id)
                match_set = subspace[subspace.cusip.isin(cusips)]
                if match_set.shape[0] > 0:
                    matched = True
                    break

            #try title checking with firm_name and title based name
            match_set = subspace[subspace.event_title.str.upper().str.contains(re.escape(t_tools.superclean(firm_name))+r'(?:\s|$)')]
            if match_set.shape[0] > 0:
                matched = True
                break
            match_set = subspace[subspace.event_title.str.upper().str.contains(re.escape(t_tools.superclean(title_name))+r'(?:\s|$)')]
            if match_set.shape[0] > 0:
                matched = True
                break

            #start fuzzy matching
            #find unique set of possible names
            firm_opts = set(subspace.firm_name)
            # compare_names runs fuzzy matching on firm_names and returns best firm name match
            # if it finds one with sufficient similarity
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
    
    if not matched:
        # file is new
        fs_new = pd.concat([fs_new, rest.to_frame().T], ignore_index=True)
    
        # update row info with today's date to reflect date when transcript was marked for processing
        fs_master.loc[fs_master.index == i, 'date_processed'] = datetime.datetime.today().strftime('%Y-%m-%d')

    # update row info to reflect that row was checked for matching
    fs_master.loc[fs_master.index == 1, 'is_duplicate'] = int(matched)

# save new fs_master file
fs_master.to_csv(f'./data_ref/fs_master_accessible.csv', index=False)

# file overwrite control        
output_path = f'{output_dir}/factset_gets_{output_name}'
while os.path.exists(output_path+'.csv'):
    output_path += '_1'
    print('outpath exists. Adding "_1" to path name')
fs_new.to_csv(f'{output_path}.csv', index=False)
print(f'Done! Found {fs_new.shape[0]} new transcripts.\nProcess took {time.time() - start_time} seconds')