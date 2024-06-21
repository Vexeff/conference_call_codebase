from collections import defaultdict
import datetime
import re
import pandas as pd
import paramiko as pk
from dotenv import load_dotenv 
from io import StringIO
import os
import zipfile


load_dotenv() 

def mercury_sftp(remote_dir: str):
    """
    Signs into mercury using RSAKey in root directory.\\
    remote_dir is the directory address on Mercury.\n
    Returns sftp object at remote_dir address.
    """
    print(f'Logging into mercury at {remote_dir}...')

    #ssh into mercury using paramiko
    host = ''
    username = str(os.environ.get("MERCURY_USERNAME"))
    ## add .env file to main folder that contains your mercury sshkey
    key = pk.RSAKey.from_private_key(StringIO(str(os.environ.get("MERCURY_KEY"))))

    ssh = pk.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, pkey=key)
    sftp = ssh.open_sftp()
    sftp.chdir(remote_dir)
    print('Mercury login succesful. Returning sftp.')
    return sftp


def get_trans_zips_info(year: str | list | None = None, date_range: tuple | list | None = None, suppress_print: bool = False):
    """
    Looks in fdsloader's zips folder for newest transcript files\
    at desired date. Additionally, checks if older zip files were larger before returning, in which case\
    it returns those instead.
    
    year: if provided, looks for transcript files from that year. \\
    Defaults to all years.
    NOTE: year can be a list, in which case it will look for that year's files.
    
    date_range: if provided, looks for files downloaded in given range. \\
    Defaults to past week. Format must be a tuple or list with start date and end date of the form yyyymmdd.

    suppress_print determines if function prints info to console.

    Returns sorted dictionary where the key is the year, and the value is the name of the largest zip file. Also returns bash script list needed for unzip.sh
    """

    # connect and get sftp object
    sftp = mercury_sftp('/project/FactSet/fdsloader/zips/')

    # start new dictionary
    new_trans = {}

    # set up date range
    if not date_range:
        # get download date range
        sd, ed = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime("%Y%m%d"), datetime.datetime.today().strftime("%Y%m%d")
    else:
        # use gien date range
        sd, ed = date_range[0], date_range[1]
    
    if not suppress_print:
        print(f'Looking for files between {sd} and {ed}')

    # set up file match pattern
    if year:
        # check if list case
        if isinstance(year, list):
            if not suppress_print:
                print(f'Looking for files in these years: {year}')
            # build match pattern
            match_pat = 'tr_history_'
            for yr in year:
                match_pat += f'({yr})|'
            # complete building match pattern
            match_pat = re.sub(r'\|$', '.*', match_pat)
        else:
            if not suppress_print:
                print(f'Looking for files in {year}')
            # build match pattern
            match_pat = f'tr_history_{year}.*'
    else:
        if not suppress_print:
            print('Looking for files from all years.')
        # build match pattern
        match_pat = 'tr_history_.*'

    # look for most recent files
    for filename in sftp.listdir():
        # only look at files of years we want
        if re.search(match_pat, filename):
            # get file metadata
            stats = sftp.lstat(filename)
            # get last modified date 
            file_date = stats.st_mtime
            if not file_date:
                ValueError(f'Was not able to find a last modified date for {filename}')
                return
            last_modified = datetime.datetime.utcfromtimestamp(file_date).strftime("%Y%m%d")
            if sd <= last_modified <= ed:
                # get years and add file and file size to dict if the file was downloaded on given day
                year = filename.split('_')[2]
                new_trans[year]= (filename, stats.st_size)

    # sort by year
    new_trans = dict(sorted(new_trans.items()))

    # initilize array for size checking proces
    years_to_pop = []
    # check for larger files from previous times
    # check size issues and only use new download 
    # if it is larger than before
    for year in new_trans:
        # get file and file size
        file, file_size = new_trans[year]
        # get file name stem for matching to other versions
        name_trim = re.sub('_full.*', '.*', file)

        # loop through other options
        for contending_file in sftp.listdir():
            if contending_file == file:
                # same file
                continue
            # other file for the same year
            if re.search(name_trim, contending_file):
                # get its size
                stats = sftp.lstat(contending_file)
                contending_size = stats.st_size
                if not contending_size:
                    ValueError(f'Was not able to find a size for {contending_file}')
                    return
                # size issue: current file is larger than previous files
                # largest file, size are always tracking largest possible options in the folder
                if contending_size >= file_size:
                    if not suppress_print:
                        print(f'{file=} cannot replace older file {contending_file}')
                    # add year to years to remove from dictionary
                    years_to_pop.append(year)
                    # move on to next year
                    break

        # remove size since it is no longer necessary
        new_trans[year] = new_trans[year][0]

    # close sftp
    sftp.close()

    # remove years from dict since they are no longer needed
    for yr in years_to_pop:
        new_trans.pop(yr)


    # translate to bash script array form for unzip.sh
    bash_array = '('
    for year in new_trans:
        file = new_trans[year]
        bash_array += f'"{file}" '
    bash_array = re.sub(r'\s$', '', bash_array)
    bash_array += ')'

    return new_trans, bash_array



def get_redo_years(new_trans: dict, suppress_print: bool = False):
    """
    Given dictionary of new transcript files unzipped, returns list\
    of new years that need to be (re)-downloaded. \\
    This is done by comparing the number of files in the 'new' folder compared to
    older folders.
    
    suppress_print determines if function prints info to console.
    """
    # connect and get sftp object
    sftp = mercury_sftp('/project/FactSet/fdsloader/unzipped_data/')
    
    # get list of folders in directory
    trans_dirs = sftp.listdir()

    # initialize new list
    new_years = []

    # compare new and old directories to find out which years need to be re-done
    for year in new_trans:

        # get name of new directory based on mercury naming pattrern
        new_dir_name = re.sub('_full.*', '', new_trans[year])

        # find name of old directory
        # match based on  year
        name_matches = list(filter(re.compile(f'.*({year}).*').match, trans_dirs))
        # remove current name from list
        name_matches = [name for name in name_matches if name != new_dir_name]
        # get old name
        if len(name_matches) > 1:
            # find largest directory
            old_dir_name, old_dir_size = '', 0
            for name in name_matches:
                size = sftp.lstat(name).st_size
                if not size:
                    ValueError(f'Was not able to find a size for {name}')
                    return []
                if size > old_dir_size:
                    old_dir_name = name
                    old_dir_size = size
        else:
            old_dir_name = name_matches[0]
        
        if not suppress_print:
            print(f'{old_dir_name=}\n{new_dir_name=}')

        old_dir_size = sftp.lstat(old_dir_name).st_size
        new_dir_size = sftp.lstat(new_dir_name).st_size

        if not old_dir_size or not new_dir_size:
            ValueError(f'Was not able to find a size for {old_dir_name=} or {new_dir_name=}')
            return []

        if new_dir_size < old_dir_size:
            print(f'Problem with {year}. Its largest zip file is not the largest unzipped.')
            return []
        if new_dir_size > old_dir_size:
            if not suppress_print:
                print(f'{year} needs to be redone: {round((new_dir_size-old_dir_size)*0.001, 2)} KB difference.')
            new_years.append(year)


    # close sftp
    sftp.close()


    return new_years


def get_all_names(years: list, dir_stem: bool = True, suppress_print: bool = False):
    """
    Given array of years, looks at those years' transcript folders
    and returns dataframe of each year's filenames.\\
    Since factset has multiple transcript versions, returns dataframe of\
    most complete transcript versions.\\
    dir_stem specifies if search should be targeted at directories beginning with 'tr_history'. \\
    Defaults to True. If False, uses plain years.

    suppress_print determines if function prints info to console.
    """

    # connect to mercury and get sftp client
    sftp = mercury_sftp('/project/FactSet/fdsloader/unzipped_data')

    # initialize dataframe
    all_filenames = pd.DataFrame()

    # loop through to get file names
    for year in years:
        # get folder name for year
        if not dir_stem:
            year_dir = year
        else:
            year_dir = f'tr_history_{year}'

        # get file names dataframe
        filenames = pd.DataFrame(sftp.listdir(year_dir), columns=['file_name'])
        # run version control
        # split filename into its separate parts
        parts = filenames.file_name.str.split('-', expand=True)
        filenames['date'] = pd.to_datetime(parts[0], format="%Y%m%d")
        filenames['report_id'] = parts[1]
        filenames['version'] = parts[2]

        #correct df for transcripts with corrections
        # get all report ids for transcripts that have corrected versions
        correction_ids = filenames[filenames.file_name.str.contains('-C')].report_id.to_list()
        # separate all rows that did not get corrected
        good_df = filenames[~filenames.report_id.isin(correction_ids)]
        # separate all rows that did get corrected
        bad_df = filenames[filenames.report_id.isin(correction_ids)]
        # filter them to only get the corrected versions
        corrected_df = bad_df[bad_df.version == 'C.xml']
        # concatenate them to get the best versioned dataset
        fixed_df = pd.concat([good_df, corrected_df]).drop_duplicates()
        
        # remove column no longer needed
        fixed_df.drop('version', axis=1, inplace=True)
        
        # add year column
        fixed_df['folder_year'] = int(year)

        if not suppress_print:
            print(f'{fixed_df.shape[0]} unique files for {year}\nOriginally had {filenames.shape[0]} files.', end='\n'+'-'*30+'\n')

        # add to overall df
        all_filenames = pd.concat([all_filenames, fixed_df])

    # close sftp
    sftp.close()

    return all_filenames


def download_metas(local_dir : str = './id_docs', suppress_print : bool = False):
    '''
    Downloads metadata files into local_dir -- defaults to './id_docs'

    suppress_print determines if function prints info to console.
    '''
    # sftp into mercury factset zips folder
    sftp = mercury_sftp('/project/FactSet/fdsloader/zips')
    
    # get list of all zip files
    file_list = sftp.listdir()

    # names of needed files
    meta_names = {'ce_events', 'ce_hub_v1', 'ent_entity_advanced_v1', 'sym_cusip_v1'}

    # dict of needed metas
    meta_dict = defaultdict(lambda: (str(''), int(0)))

    for meta in meta_names:
        # get list of possible files to choose from
        meta_opts = list(filter(re.compile(f'{meta}_full.*').match, file_list))
        # loop through to find most recent one
        for meta_file in meta_opts:
            stats = sftp.lstat(meta_file)
            # get last modified date 
            meta_date = stats.st_mtime
            if not meta_date:
                ValueError(f'Was not able to find a last modified date for {meta_file}')
                break
            # update file if size is bigger
            if meta_date > meta_dict[meta][1]:
                meta_dict[meta] = (meta_file, meta_date)

    # download the files into local_di
    for key in meta_dict:
        filename = meta_dict[key][0]
        local_path = f'{local_dir}/{filename}'
        if os.path.exists(local_path):
            print(f'{filename} already exists. Skipping it.')
            continue
        sftp.get(filename, local_path)
        print(f'Done downloading {filename}')
        # unzip the file
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extractall(local_dir)
    

    # delete unwanted files
    for files in os.listdir(local_dir):
        if files not in ['ce_reports.txt', 'ce_events.txt', 'ent_entity_coverage.txt', 'ce_sec_entity.txt', 'sym_cusip.txt']:
            try:
                os.remove(f'{local_dir}/{files}')
            except:
                print(f'Could not delete {files}')

        




