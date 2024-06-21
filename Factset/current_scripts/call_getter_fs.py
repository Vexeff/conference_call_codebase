import pandas as pd
import modules.xml_mods as xml_tools
import os
import time

# address to universe of factset transcripts
trans_univ = '/project/FactSet/fdsloader/unzipped_data'
# address to save directory
output_dir = '/project/kh_mercury_1/conference_call/xml_pulls/output/factset'

# pick file output name -- e.g. 2023_q3 will show up as '...transcripts_2023_q3'
output_name = '2024_q1'

#update to most recent factset get dataset
get_df = pd.read_csv('/project/kh_mercury_1/conference_call/xml_pulls/data_ref/get_files/factset_gets_2024_q1.csv')
get_df['text'] = ''

#start timer
start_time = time.time()

# change directory to transcript universe
os.chdir(trans_univ)

# start loop to download files
for i, rest in get_df.iterrows():
    # get individual file name
    file_name = rest.file_name
    # get folder year
    year = rest.Year
    # convert to year's folder name
    year_dir = f'tr_history_{year}'
    # create absolute file path
    file_path = f'{trans_univ}/{year_dir}/{file_name}'
    # get text body
    res = xml_tools.get_xml_body(ref_con=None, path=file_path, db='fs')
    # save it to the relevant row
    get_df.loc[get_df.index == i, 'text'] = res

# file overwrite control        
output_path = f'{output_dir}/factset_transcripts_{output_name}'
while os.path.exists(output_path+'.csv'):
    output_path += '_1'
    print('outpath exists. Adding "_1" to path name')
    
get_df.to_csv(f'{output_path}.csv', index=False)
print(f'Done! Whole Process took {time.time()-start_time} seconds.')
