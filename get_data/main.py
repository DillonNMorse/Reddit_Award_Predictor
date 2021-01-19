# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:30 2021

@author: Dillo
"""

import os
import time
from datetime import datetime as dt

import pandas as pd

import exceptions
from pull_posts import pull_posts
from retrieve_comments import  process_all_posts_comments
from extract_features import submission_features



def get_reddit_submissions(sortedby = 'new',
                           num_posts = 500,
                           num_top_comments = 15,
                           subreddits_fpath = './subreddits.txt',
                           reddit_auth_file = '../auth.txt',
                           savepath = './data/'
                           ):
    """
    Wrapper which structures the pull_and_process procedure. Iterates over all
    sortedby types, pulls and processes data, adds to Pandas dataframe, then
    saves as a pickle file.
    
    Each iteration within sortedby can be time consuming and the API may
    timeout during. The number of retry attempts per sortedby value is set
    to 3 with a 60 second delay between attempts.

    Parameters
    ----------
    sortedby : list, optional
        List of methods by which the posts will sorted within Reddit. Sorted 
        by types include "new", "hot", "rising". The default is ['new'].
    num_posts : int, optional
        Number of posts to pull. Provides and upper bound, may end up with 
        fewer, unsure of why but I blame the API. The default is 500.
    num_top_comments : int, optional
        How many of the comments (sorted by number of upvotes, descending) will
        be pulled for analysis. The default is 15.
    subreddit_list : list, optional
        List containing strings defining subreddits to include within the 
        model. A default list is currently hard coded The default is 
        subreddit_list.
    reddit_auth_file : str, optional
        Filepath of .txt file containg Reddit keys. The default is 'auth.txt'.
    savepath : str, optional
        Defines directory where pickle file will be saved. The default is 
        './data/'.

    Returns
    -------
    None. A file with the scraped data will be saved to disk in the designated
    directory with a unique filename specifying the parameters used. E.g.
    "./data/01-15_at_1530utc_sortedby_new_using_10_comments.pkl"

    """
    
    #subreddit_list = get_subreddit_list(subreddits_fpath)
    pulled_post_data = pull_posts(reddit_auth_file = reddit_auth_file,
                                  subreddits_fpath = subreddits_fpath,
                                  num_posts = num_posts,
                                  how = sortedby,
                                  num_top_comments = num_top_comments
                                 )
    subms_fpath, subm_IDs, auth, working_directory = pulled_post_data
    
    
    submission_features(auth, subms_fpath, working_directory)
    
    process_all_posts_comments(subms_fpath, subm_IDs, auth, working_directory)
    
    
    
# =============================================================================
#     if not os.path.isdir(savepath):
#         os.mkdir(savepath)
#     
#     subreddit_list = get_subreddit_list(subreddits_fpath)
#     
#     max_tries = 3
#     secs_betw_attempts = 60
#     secs_betw_sortedby_types = 30
#     num_sortedby_types = len(sortedby)
#     
#     for j, how in enumerate(sortedby):
#         num_tries = 0
#         data_obtained = False
#         while not data_obtained:
#             num_tries += 1
#             if num_tries > max_tries:
#                 raise exceptions.ApiRetryAttemptsExceeded("""API timed out 
#                                                           after {} tries"""
#                                                           .format(max_tries)
#                                                           )
#             try:
#                 data = pull_posts(reddit_auth_file = reddit_auth_file,
#                                         subreddit_list = subreddit_list,
#                                         num_posts = num_posts,
#                                         how = how,
#                                         num_top_comments = num_top_comments
#                                        )
#                 df = pd.DataFrame(data).transpose()
#             
#                 fname = build_filename(sort_and_comments = True,
#                                        sortedby = how,
#                                        num_top_comments = num_top_comments,
#                                        )
#                 file_path = os.path.join(savepath, fname)
#                 df.to_pickle(file_path)
# 
#                 data_obtained = True
#             except:
#                 time.sleep(secs_betw_attempts)
#         if not (j ==  num_sortedby_types - 1):
#             time.sleep(secs_betw_sortedby_types)
# =============================================================================
                
    return None




def get_subreddit_list(subreddits_filepath):
    """
    Reads and parses txt file containing all subreddits to include in analysis.

    Parameters
    ----------
    subreddits_filepath : str
        Filepath of txt file containing list of subreddits, delimited by ',/n'.

    Returns
    -------
    subreddit_list: list
        List of all subreddits.

    """
    with open(subreddits_filepath, "r") as f:
        subreddit_list = f.read().split(',\n')
        
    return subreddit_list




def build_filename(sort_and_comments = False, sortedby = None, num_top_comments = None):
    """
    Constructs a unique filename based on parameters chosen to pull from API.

    Parameters
    ----------
    sort_and_comments : bool, optional
        Whether to include information about how the posts were sorted ('new',
        'rising', etc.) and the number of top-level comments used in the
        analysis. The default is False.
    sortedby : str, optional
        How the posts were sorted within Reddit ('new', 'hot', etc.). The 
        default is None.
    num_top_comments : int or None, optional
        The number of top-level comments used in the analysis. The default is 
        None.

    Returns
    -------
    filename : TYPE
        DESCRIPTION.

    """
    if sort_and_comments:
        if (sortedby == None) | (num_top_comments == None):
            raise exceptions.InsufficientDataPassed("""Must include sortedby 
                                                    and num_toplevel_comments
                                                    values if choosing to use
                                                    sort_and_comments info
                                                    for the filename""")
    
    t1 = dt.utcnow()

    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]
    filename_dtime = month + '-' + day + '_at_' + hour + minute + 'utc'

    
    if sort_and_comments:
        filename = ( filename_dtime + 
                    '_sortedby_' + sortedby + 
                    '_using_' + str(num_top_comments) + '_comments' +
                    '.pkl'
                    )
    else:
        filename = filename_dtime + '.pkl'
    
    
    return filename