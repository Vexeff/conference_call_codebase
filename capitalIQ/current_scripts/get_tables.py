import datetime
import numpy as np
from modules.wrds_mod import Connection_remote
import pandas as pd    
import dotenv
import time

dotenv.load_dotenv()

# add your wrds username, password, and hostname to .env file

WRDS_USERNAME = dotenv.get_key('./.env', 'WRDS_USERNAME')
WRDS_PASSWORD = dotenv.get_key('./.env', 'WRDS_PASSWORD')
WRDS_HOSTNAME = dotenv.get_key('./.env', 'WRDS_HOSTNAME')
WRDS_PORT = 9737

start_time = time.time()

db = Connection_remote(wrds_hostname = WRDS_HOSTNAME,
                     wrds_port =  WRDS_PORT,
                     wrds_username = WRDS_USERNAME, 
                     wrds_password = WRDS_PASSWORD,
                     autoconnect = False)

db.connect()
db.load_library_list()

# import ciq master dataset
ciq_master = pd.read_csv('../data_ref/ciq_master.csv')

# custom helper apply function to aggregate grouping 
# based on most recent transcript creation date
def recency_agg(x):
      # gets most recent version of given transcript
      most_recent_sub = x.loc[x.transcript_timestamp.idxmax(), :]
      # gets most recent transcript id 
      trans_id = most_recent_sub.transcriptid
      # gets most recent transcript timestamp
      transcript_timestamp = most_recent_sub.transcript_timestamp
      # gets most recent transcript text
      text = most_recent_sub.componenttext
      # return most recent as a row
      return pd.Series({'transcriptid': trans_id, 'transcript_timestamp': transcript_timestamp, 'text': text})


# here to allow multi-year downloading if necessary
for yr in range(2001, 2025):
      # skip if already downloaded
      if yr < 2024:
            continue
      # start time
      yr_start_time = time.time()
      
      # sql query from capital IQ servers
      sql_query = f'''
                  SELECT a.companyid, a.transcriptid, a.headline, a.mostimportantdateutc, a.companyname, c.componenttext, d.gvkey, \
                  a.transcriptcollectiontypeid,	a.transcriptcollectiontypename, a.transcriptcreationdate_utc, a.transcriptcreationtime_utc \
                  FROM (SELECT * \
                        FROM ciq.wrds_transcript_detail \
                        WHERE date_part('year',mostimportantdateutc) in ({yr})) AS a\
                  LEFT JOIN ciq.wrds_gvkey as d ON a.companyid = d.companyid\
                  JOIN ciq.wrds_transcript_person as b ON a.transcriptid = b.transcriptid\
                  JOIN ciq_transcripts.ciqtranscriptcomponent AS c ON b.transcriptcomponentid = c.transcriptcomponentid \
                  ORDER BY a.transcriptid, b.componentorder;'''
      
      # run query
      data = db.raw_sql(sql_query)

      # check if year has data (should not trigger in later years)
      if not data.shape[0]:
            print(f'No data in year {yr}. Continuing...')
            continue
      print(f'Year {yr} took {time.time() - yr_start_time} seconds to download!')

      # simplify and collapse
      # adding transcript creation timestamp for version control
      data['transcript_timestamp'] = pd.to_datetime(data['transcriptcreationdate_utc'].astype(str) + ' ' + data['transcriptcreationtime_utc'].astype(str))

      # combine texts of all ciq calls to combine components
      ciq_agg = {'headline': 'first', 'mostimportantdateutc': 'first', 'companyname': 'first',
            'gvkey': lambda x: ','.join(str(y) for y in set(sorted(x.dropna().to_list()))),
            'componenttext': lambda x: ('\n'+20*'-'+'\n').join(x.dropna())}

      # group dataframe by transcript identifiers
      ciq_df = data.groupby(['transcriptid', 'companyid', 'transcript_timestamp']).agg(ciq_agg).reset_index()

      # group to get most recent version of transcript
      grouped_ciq = ciq_df.groupby(['companyid', 'companyname', 'gvkey', 'headline', 'mostimportantdateutc']).apply(lambda x: recency_agg(x)).reset_index()
      grouped_ciq.rename({'companyname': 'firm_name', 'headline': 'event_title', 'mostimportantdateutc': 'event_date'}, axis=1, inplace=True)

      # Note: transcriptid and companyid TOGETHER uniquely identify transcripts. 
      # Discrepancy is because multiple companies in the same event leads to multiple copies of the transcript, but same transcriptid.
      # Capture number of cases where each transcript is duplicated in a separate column
      grouped_ciq['num_entries'] = grouped_ciq.groupby('transcriptid')['transcriptid'].transform(len)

      print(f'{data.shape[0]=}')
      print(f'{grouped_ciq.shape[0]=}')
      
      # save ciq dataset for the year
      grouped_ciq.to_csv(f'../output/transcript_data/{yr}_ciq_trans_cleaned.csv', index=False)

      # add metas to overall dataset for ease of use
      df = grouped_ciq.drop(columns=['text', 'num_entries', 'transcript_timestamp'])
      # add today's download date to file
      df['date_added'] = datetime.datetime.today().strftime('%Y-%m-%d')
      
      # standardize data types
      df.companyid = df.companyid.astype(int)
      df.transcriptid = df.transcriptid.astype(int)
      
      # add it to overall dataset, drop duplicates that already exist
      ciq_master_new = pd.concat([ciq_master, df]).sort_values('date_added', ascending=False).drop_duplicates(subset=['transcriptid', 'companyid']).reset_index(drop=True)
      print(f'{ciq_master_new.shape[0] - ciq_master.shape[0]} new files added from {yr} download.')
      
      # save overall dataset
      ciq_master_new.to_csv('../data_ref/ciq_master.csv', index=False)


db.close()
print(f'Done! Whole process took {time.time() - start_time} seconds')


