import re
import pandas as pd
import os

# Import keywords
keywords = pd.read_csv('./data_ref/keywords.txt', sep = "\t", header = None)[0]

def mysplit(para):
    '''
    Splits given text, para, into separate lines for ease of looking.
    Typically this will take the form of paragraph splits.
    '''
    splt =  re.split('\n|\n\r|\r', para)
    paras = [p for p in splt if len(p) > 10]
    return paras

def kw_search(kw, para):
    '''
    Given keyword kw, returns true if it exists in paragraph para.
    '''
    kw = kw.upper()
    para = para.upper()
    padder = r'\b'
    pattern = padder+kw+padder
   
    return re.search(pattern, para)

# helps split large paras
def smart_split(para):
    ''' Helper function for check_and_fix_large_paras'''
    buffer = []
    while len(para) > 32765:
        end_ind = 32765
        max_chunk = para[:end_ind]
        rev = max_chunk[::-1]
        end_search = re.search(r'\.', rev)
        if end_search:
            end_ind = len(max_chunk)-end_search.span()[1]+1
        chunk = para[:end_ind]
        buffer.append(chunk)
        para = para[end_ind:]
    buffer.append(para)
    return buffer

# splits max length paras into extra rows to stay within
# excel's char limit
def check_and_fix_large_paras(df):
    '''
    Given dataframe of paragraphs, splits them up 
    if the size of the paragraphs is larger than excel's maximum 
    character limit.
    '''
    # empty df protection
    if not df.shape[0]:
        return df
    # find paragraphs whose char number is bigger than excel's limit
    probs = df[df.Paragraph.str.len() >= 32767].copy()
    # enter correction loop if such paragraphs exist
    if probs.shape[0] > 0:
        # save paragraphs whose char number is within limit
        save = df[df.Paragraph.str.len() < 32767].copy()
        # initialize dataframe used to fix issue
        fixes = pd.DataFrame()
        # start fixing the large paragraphs
        for i, row in probs.iterrows():
            # create array of smaller chunks of the paragraph
            buffer = smart_split(row.Paragraph)
            prob_fixes = pd.DataFrame()
            # re attach fixed paragraphs
            for i, chunk in enumerate(buffer):
                new_row = row.copy()
                new_row['Paragraph'] = chunk
                prob_fixes = pd.concat([prob_fixes, new_row.to_frame().T], ignore_index=True)
            
            fixes = pd.concat([fixes, prob_fixes])
        return pd.concat([save, fixes], ignore_index=True)
    else:
        return df

def paragraph_contains_pc_bp(para):
    ''' Given paragraph, returns true if 
    it contains % sign, per cent, percent, percentage, 
    basis point, bp, or bps.'''
    padder = r'\b'
    bp = padder+'bp'+padder
    bps = padder+'bps'+padder
    para = para.lower()
    num_list = ["%", "per cent", "percent", "percentage", "basis point", bp, bps]
    for i in num_list:
        if re.search(i, para):
            return True
    return False

def get_all_paras_containing_keywords_from_a_conf_call(row, keywords, check_for_nums = True):
    '''
    Given row in a dataframe of calls, returns expanded dataframe of \
    paragraphs from the call that have keywords. 
    Note: if check_for_nums is true (default), checks for a percent or basis point sign
    in a paragraph before adding it.
    '''
    # Split conference call into paragraphs.
    call = str(row["text"])
    paras_list = mysplit(call)
    found_keywords, found_in_paras = [], []

    file_name = row.file_name
    for para in paras_list:
        for keyword in keywords:
            # Check if keyword is in paragraph
            if kw_search(keyword, para):
                # if percent/number needs to exist, 
                # check for those before adding
                if check_for_nums:
                    if paragraph_contains_pc_bp(para):
                        found_keywords.append(keyword)
                        found_in_paras.append(para)
                    continue
                # if no percent check, add
                found_keywords.append(keyword)
                found_in_paras.append(para)
    
    # Create df 
    # Note that found_keywords and found_in_paras are lists. report_id is an int, but it will be broadcasted to a list.
    df = pd.DataFrame({"Keyword": found_keywords, "Paragraph": found_in_paras, "file_name": file_name})
    return df 

def get_all_paras_containing_keywords_from_a_csv_file(df_csv_file, check_for_nums = True):
    '''
    Given dataframe of transcripts with call text included, returns dataframe
    of paragraphs found with keywords -- as detailed in the data_ref folder. 
    
    Note: if check_for_nums is true (default), checks for a percent or basis point sign
    in a paragraph before adding it.
    '''
    df_combined = pd.DataFrame()
    for _, row_conf_call in df_csv_file.iterrows():
        df = get_all_paras_containing_keywords_from_a_conf_call(row_conf_call, keywords, check_for_nums=check_for_nums)
        df_combined = pd.concat([df_combined, df])
    return df_combined



def prepare_all_files(filenames: list,  db: str, suppress_print: bool = False, check_for_nums: bool = True):
    '''
        Given list of names of new transcript csv file names and name of database, \\
        extracts paragraphs, renames and prepares columns for paragraph division.

        db must be one of 'fs', 'ref', or 'ciq'.

        suppress_print determines if progress is written to console.

        Note: if check_for_nums is true (default), checks for a percent or basis point sign
        in a paragraph before adding it.
    '''
    # defend against unexpected db commands
    if db not in ['ref', 'fs', 'ciq']:
        raise ValueError(f"{db} is not a valid database name.\ndb must be one of 'fs', 'ref', or 'ciq'")
    
    # get database directory folder from db input
    if db == 'ref':
        db_dir = 'Refinitiv'
    elif db == 'fs':
        db_dir = 'FactSet'
    else:
        db_dir = 'CapitalIQ'
    for filename in filenames:
        # check if files actually exist
        if not os.path.exists(f'./new_calls/{db_dir}/{filename}'):
            raise ValueError(f'{filename} not found in new_calls/{db_dir}.')
            
    if not suppress_print:
        print('Starting paragraph extraction...')

    # initialize final dataframe
    combined = pd.DataFrame()
    # start process
    for filename in filenames:
        # import calls dataset
        df = pd.read_csv(f'./new_calls/{db_dir}/{filename}')
        
        # ciq is not file-based, so if source is ciq, 
        # call the file name the report id for consistency across sources
        if db == 'ciq':
            df['file_name'] = 'transcriptid_'+df.transcriptid.astype(int).astype(str)
        
        # extract paras and merge with transcript metadata dataset
        new_df = get_all_paras_containing_keywords_from_a_csv_file(df, check_for_nums=check_for_nums).merge(df, on='file_name').drop(columns='text').reset_index(drop=True)
        
        # renames reformat to standardize columns
        if db == 'fs':
            # factset case
            # rename columns
            new_df.rename(columns={'Year': 'folder_year', 'ENTITY_PROPER_NAME': 'Firm_name', 'TITLE': 'Subtitle', 'EVENT_DATETIME_UTC': 'Date', 'ISO_COUNTRY': 'country'}, inplace=True)
            # change report id to differentiate across sources
            new_df['Report'] = 'fs_'+new_df.REPORT_ID.astype(str)
            # add empty gvkey column since it does not exist
            new_df['gvkey'] = ''
        elif db == 'ref':
            # refinitiv case
            # rename columns
            new_df.rename(columns={'firm_cusip': 'CUSIP', 'firm_id': 'CUSIP', 'cusip': 'CUSIP', 'firm_name': 'Firm_name', 'event_title': 'Subtitle', 'event_date': 'Date'}, inplace=True)
            # get report id to differentiate across sources
            new_df['Report'] = new_df.file_name.str.split('_').str[0]
            # add empty gvkey column since it does not exist
            new_df['gvkey'] = ''
        else:
            # capital iq case
            # rename columns
            new_df.rename(columns={'Year': 'folder_year', 'firm_name': 'Firm_name', 'event_title': 'Subtitle', 'event_date': 'Date'}, inplace=True)
            # change report id to differentiate across sources
            new_df['Report'] = 'ciq_'+new_df.transcriptid.astype(int).astype(str)
            # add empty cusip column since it does not exist
            new_df['CUSIP'] = ''
        
        # fix date type column
        new_df['Date'] = pd.DatetimeIndex(new_df.Date).date
        
        # select only needed columns
        new_df = new_df[['Keyword', 'Paragraph', 'file_name', 'folder_year', 'CUSIP', 'gvkey', 'Firm_name', 'Subtitle', 'Date', 'Report']]
        
        # split up very large paragraphs
        try:
            new_df = check_and_fix_large_paras(new_df)
        except:
            print('ffs')
            return new_df
        
        # drop duplicates
        new_df.drop_duplicates(inplace=True)

        if not suppress_print:
            print(f'{filename} led to {new_df.shape[0]} paragraphs.')

        # add to overall dataframe
        combined = pd.concat([combined, new_df]).reset_index(drop=True)

    return combined

    

        




