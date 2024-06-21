import paramiko as pk
import os
import time

start_time = time.time()

# path to refinitiv universe of filels
REF_UNIV = '/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/'

# year of data
data_year = 2024

# if new folder does not exist in our directory, create it
if str(data_year) not in os.listdir(REF_UNIV):
    os.mkdir(REF_UNIV + str(data_year))

# change to this path locally
os.chdir(REF_UNIV + str(data_year))

# set up login for refinitiv:
host = ''
port = 22
username = ''
password = ''

# ssh into refinitiv
ssh = pk.SSHClient()
ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
ssh.connect(hostname = host, 
            username=username, 
            password=password,
            look_for_keys = False,
            banner_timeout=200,
            timeout=200,
            auth_timeout=200)

# open sftp protocol
sftp = ssh.open_sftp()

# Changing to refinitiv directory we want to copy
sftp.chdir(f'./TRANSCRIPT/XML_Add_IDs/Current')

# get list of file names
files = sftp.listdir()

# get current list of file names
curr_files = os.listdir()

# track # of files
cnt = 0
for filepath in files:
    # don't overwrite if file exists
    if filepath in curr_files:
        print('Skipping ' + filepath)
        continue
    sftp.get(filepath, filepath)
    cnt += 1

sftp.close()

# change directory back to codebase to keep log location correct
os.chdir('/project/kh_mercury_1/conference_call/xml_pulls/codebase')
print(f'Done. Copied {cnt} files.\nCopying took {time.time() - start_time} seconds')
