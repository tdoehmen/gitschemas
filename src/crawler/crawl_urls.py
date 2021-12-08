'''
Script for crawling SQL files from GitHub. Make sure to fill in a valid USER and API TOKEN.
Gets all URLs of SQL files that contain the words "CREATE TABLE FOREIGN KEY" and stores them in
a checkpoint file.

Thanks to EleutherAI and madelonhulsebos. The script was heavily inspired by their work.
https://github.com/EleutherAI/github-downloader
https://github.com/madelonhulsebos/gittables
'''

import os
import time
import math
import pickle
import random

import requests
import logging

#~~~~~~~~~~~~~~~~~~
USER = "user" # fill in valid GitHub USER
TOKEN = "xxx" # fill in valid GitHub TOKEN
#~~~~~~~~~~~~~~~~~~


def save_ckpt(lower_bound: int, upper_bound: int):
    global url_list
    url_list = list(set(url_list)) # remove duplicates
    print(f"Saving checkpoint {lower_bound, upper_bound}...")
    with open('../../data/repo_ckpt.pkl', 'wb') as f:
        pickle.dump((lower_bound, upper_bound, url_list), f)

def get_request(lower_bound: int, upper_bound: int, page: int = 1):
    # Returns a request object from querying GitHub
    # for repos in-between size lower_bound and size upper_bound with over 100 stars.
    global USER, TOKEN, url_list

    time.sleep(random.randint(8, 12))

    r = requests.get(
        f'https://api.github.com/search/code?q=create+table+foreign+key+language:SQL+size'
        f':{lower_bound}..{upper_bound}&per_page=100&page={page}',
        auth = (USER, TOKEN)
    )

    if r.status_code == 200:
        return r
    if r.status_code == 403:
        save_ckpt(lower_bound, upper_bound)
        headers = r.headers

        if "Retry-After" in headers:
            wait_time = int(headers["Retry-After"])
        elif "X-RateLimit-Reset" in headers:
            # Overwrite waiting time if the rate limit was hit.
            wait_time = int(headers["X-RateLimit-Reset"]) - time.time()
        else:
            wait_time = 60

        wait_time = max([1, wait_time])

        print(f'API rate limit exceeded. Waiting for: {wait_time}s')
        time.sleep(wait_time)

        # try again
        return get_request(lower_bound, upper_bound, page)
    elif r.status_code == 422:
        # No more pages available
        return False
    else:
        print(f'Unexpected status code. Status code returned is {r.status_code}')
        print(r.text)
        save_ckpt(lower_bound, upper_bound, url_list)
        print("Exiting program.")
        exit()


def download_range(lower_bound, upper_bound):
    # Saves the names of repositories on GitHub to repo_list
    # in-between size minimum and maximum with over 100 stars.
    global url_list, _logger
    # Github page options start at index 1.
    for page in range(1, 11):
        r = get_request(lower_bound=lower_bound, upper_bound=upper_bound, page=page)

        if page == 1:
            n_results = r.json()['total_count']
            print(f"Results: {n_results}")
            _logger.info(
                "lower_bound %s; upper_bound: %s; n_results: %s",
                lower_bound,
                upper_bound,
                n_results
            )
            n_query_pages = min(math.ceil(n_results/100), 10) # GitHub API capped at 1000 results

        print(f"... page {page} / {n_query_pages}")

        items = r.json()["items"]
        url_list = url_list + [item["html_url"] + "?raw=true" for item in items]

        if page >= n_query_pages:
            # No more pages available
            return n_results

def get_current_step_size(current):
    # step start and size (based on sampled file size distribution)
    step_sizes = [(0,99,100),(100,199,10),(200,499,3),(500,999,2),(1000,2499,4),(2500,4999,10),
                  (5000,9999,25),(10000,24999,100),(25000,99999,250),(100000,400000,1000)]
    for min, max, step in step_sizes:
        if min <= current <= max:
            return step
    #should never return
    return 10

if __name__ == '__main__':
    logging_filepath = f"../../data/extraction_logfile.log"
    if not os.path.exists(logging_filepath):
        open(logging_filepath, "w+")

    logging.basicConfig(filename=logging_filepath, filemode="a", level=logging.INFO)
    global _logger
    _logger = logging.getLogger()

    # If pickled checkpoint exists, load it.
    # Otherwise, intialize repo_list as an empty list
    if '../../data/repo_ckpt.pkl' in os.listdir():
        # Load checkpoint
        with open('../../data/repo_ckpt.pkl', 'rb') as f:
            lower_bound, upper_bound, url_list = pickle.load(f)
        print(f"Loading from {lower_bound}..{upper_bound}")

        print(f"Downloading repositories in size range {lower_bound}..{upper_bound}")
        download_range(lower_bound, upper_bound)
    else:
        lower_bound = 0
        upper_bound = 0
        url_list = []

    if lower_bound >= 400000:
        print('''
Checkpoint is for an already completed download of GitHub repository information.
Please delete `repo_ckpt.pkl` to restart and try again.
            ''')
        exit()

    # Main loop.
    # Breaks when all repositories considered are greater in size than a gigabyte
    while lower_bound < 400000:
        # Search for a range of repository sizes
        # from the current lower bound that has <= 1000 repositories
        lower_bound = upper_bound + 1
        upper_bound = lower_bound + (get_current_step_size(lower_bound)-1)

        print(f"Downloading repositories in size range {lower_bound}..{upper_bound}")
        download_range(lower_bound, upper_bound)

    save_ckpt(lower_bound, upper_bound)

