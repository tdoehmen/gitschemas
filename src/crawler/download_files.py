'''
This script downloads all files occuring in the checkpointed URL list.

Note: The script was written for Windows Power Shell. Sorry.
'''

import os
import pickle
from joblib import Parallel, delayed


def download_file(i, file_url):
    file_name = f'{i:06d}' + '_' + file_url.split("/")[-1].replace("?raw=true", "")
    os.system(f'powershell -command "Invoke-WebRequest {file_url} -O ..\\..\\data\\sqlfiles\\{file_name}"')


def download_files(sql_files):
    Parallel(n_jobs=40, prefer="threads")(delayed(download_file)(i, file_url) for i, file_url in
                                          enumerate(sql_files))

if __name__ == '__main__':
    global filenames_count
    filenames_count = {}
    # If pickled checkpoint exists, load it.
    # Otherwise, intialize repo_list as an empty list
    if '../../data/repo_ckpt.pkl' in os.listdir():
        # Load checkpoint
        with open('../../data/repo_ckpt.pkl', 'rb') as f:
            lower_bound, upper_bound, url_list = pickle.load(f)

        with open('../../data/github_sqls.csv', 'a') as f:
            for i, url in enumerate(url_list):
                f.write(f'{i:06d},{url}\n')

        download_files(url_list)
