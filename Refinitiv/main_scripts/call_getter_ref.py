import datetime
import pandas as pd
import modules.xml_mods as xml_tools
import modules.title_tools as t_tools 
import os
import time

start_time = time.time()

# address to local refinitiv database
trans_univ = '/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/'
# address to output directory for refinitix xmls
save_dir = '/project/kh_mercury_1/conference_call/xml_pulls/output/refinitiv/'

# change this to year where data is being collected -- assumption that should be maintained is that folder names
# are the same across trans_univ and save_dir
year = 2024
# change to suffix you would like to assign this period. i.e. q2 leads to 2023_q2 in final file save name
save_name = 'q1'

# import master dataset of all refinitiv xmls previously gotten
ref_master = pd.read_csv('/project/kh_mercury_1/conference_call/xml_pulls/data_ref/ref_master.csv', dtype=str)
old_list = ref_master.file_name.to_list()

# change to transcript saving universe
os.chdir(trans_univ)

# get list of all files in folder to download
file_list = os.listdir(str(year))

all_files = []

files = 0
for fil in file_list:
    # skip non-xml files
    if 'xml' not in fil:
        continue
    # skip if file already in our dataset
    if fil in old_list:
        continue
    # get xml metas
    metas = xml_tools.get_xml_metas(None,f'{year}/'+fil)
    if not metas:
        continue
    # get firm_name in call
    firm_name = metas[1]
    # get title of call
    title = metas[2]
    # get date of call
    date = metas[3]
    # skip if it's a government report
    if 'United States of America (Government)' in firm_name or 'Government' in firm_name:
        continue
    #add other metadata to file row
    metas.insert(0, fil)
    metas.insert(0, year)
    text = t_tools.clean_ref_body(xml_tools.get_xml_body(None,f'{year}/'+fil, 'ref'))
    metas.append(text)
    all_files.append(metas)
    files += 1

# create dataframe of all new transcripts    
# note: removed isin recording for now
df = pd.DataFrame(all_files, columns=['folder_year', 'file_name', 'cusip', 'firm_name', 'event_title', 'event_date', 'text'])
# add today's date in a column for record keeping purposes
df['date_added'] = datetime.datetime.today().date().strftime('%Y-%m-%d')
# save file 
# protect against file over-writing
file_name = f'{save_dir}/{year}_{save_name}_transcripts'
while os.path.exists(file_name+'.csv'):
    print(f'path {file_name} already exists -- adding "_1" to name before saving.')
    file_name += '_1'
df.to_csv(f'{file_name}.csv', index=False)
print(f'Done! Downloading process took {time.time()-start_time} seconds.\nFound {files} new files')


# update data_ref files
print('Creating updated overall datasets...')

# get only useful columns from new df 
df = df[['file_name', 'cusip', 'firm_name', 'event_title', 'event_date', 'date_added']].copy()

# update ref_master
new_ref_master = pd.concat([ref_master, df]).sort_values('date_added').drop_duplicates(subset='file_name').reset_index(drop=True)

print(f'{new_ref_master.shape[0]-ref_master.shape[0]} additional transcripts added to ref_master dataset.')

#save new dataset of all refinitiv xmls
new_ref_master.to_csv('/project/kh_mercury_1/conference_call/xml_pulls/data_ref/ref_master.csv', index=False)
