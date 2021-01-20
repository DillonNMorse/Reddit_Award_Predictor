# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:44:35 2021

@author: Dillo
"""

import os
from datetime import datetime as dt

import praw
import pickle



def pull_posts(reddit_auth_file,
               subreddits_fpath,
               num_posts, 
               num_top_comments = 15,
               how = 'new',
               working_directory = './working'
              ):
    """
    Makes API call, saves all submissions to disc for later processing.

    Parameters
    ----------
    reddit_auth_file : str
        Filename of .txt file containing Reddit API keys.
    subreddits_fpath : str
        Filepath pointing to text file containing ',/n' separated subreddit
        names.
    num_posts : int
        The total number of submissions to be retrieved.
    num_top_comments : int, optional
        How many of the submission's comments (sorted by comments' upvotes,
        descending) should be used in the analysis. Younger submissions may not
        yet have many comments. The default is 15.
    how : str, optional
        How the submissions are to be sorted within Reddit. The default is 
        'new'.
    working_directory: str, optional
        The path of the directory to save the submissions

    Returns
    -------
    fpath: str
        Relative filepath where submissions were saved
    submission_IDs: list
        List of strings, the ID's of all posts pulled
    auth: auth_object
        Holds keys necessary for accessing Reddit API
    working_directory: str
        The directory where intermediate files will be saved  
    """
    
    if not os.path.isdir(working_directory):
        os.mkdir(working_directory)
    
    # Pull Reddit API keys from file
    auth = get_auth(reddit_auth_file)
    
    # Instantiate a Reddit API call header
    reddit = praw.Reddit(client_id = auth.client_id,
                         client_secret = auth.client_secret,
                         user_agent = auth.user_agent,
                         )
    
    # Convert subreddits file to a string compatible with Reddit API
    subreddit_list = get_subreddit_list(subreddits_fpath)
    reddit_subs_to_pull = subreddit_list_to_string(subreddit_list)
    
    # Instantiate subreddit instance. Lazy, doesn't hit API until it is used.
    subreddit_instance = getattr(reddit.subreddit(reddit_subs_to_pull),
                                 how
                                )(limit = num_posts)
    
    # Put all submissions in to a list, this forces the API call    
    submissions = []
    submissions_IDs = []
    for subm in subreddit_instance:
        submissions.append(subm)
        submissions_IDs.append([subm.id, str(subm.subreddit)])
    
    # Save list of Reddit submissions to disc
    fpath = build_working_filename(working_directory, how)
    pickle.dump(submissions, open(fpath, 'wb'))

      
    return fpath, submissions_IDs, auth, working_directory




class get_auth:
    """
    Convert a txt file containing Reddit API keys in to an authorization
    object containing necessary keys, etc.

    Parameters
    ----------
    reddit_auth_file : str
        Filepath of .txt file containing Reddit keys, see sample_auth.txt

    Returns
    -------
    auth : auth_object
        Authorization object with properites needed to access the Reddit API.
    """

    def __init__(self, reddit_auth_file):
        f = open(reddit_auth_file, "r")
        for var, line in enumerate(f):
            if len(line.split('=')) < 2:
                pass
            else:
                variable_name = line.split('=')[0].strip()
                variable_value = line.split('=')[1].strip()
                
                if variable_name == 'client_id':
                    self.client_id = variable_value
                elif variable_name == 'client_secret':
                    self.client_secret = variable_value
                elif variable_name == 'user_agent':
                    self.user_agent = variable_value                
        f.close()             




def subreddit_list_to_string(subreddit_list):
    """
    Converts a list of subreddits to a single string for passing to Reddit API.

    Parameters
    ----------
    subreddit_list : list
        A list of all subreddits to be included in the analysis.

    Returns
    -------
    subreddit_multi_name: str
        A string containing all subreddits to be analyzed, for use in Reddit
        API.
    """
    
    subreddit_multi_name = ''
    for subreddit_name in subreddit_list:
        subreddit_multi_name += '+' + subreddit_name.strip()
    
    return subreddit_multi_name[1:]




def build_working_filename(fpath, how):
    """
    Builds unique filename for storage of intermediate data.

    Parameters
    ----------
    fpath : str
        The directory where intermediate files are stored.
    how : str
        How the Reddit is sorting posts, e.g. 'new'

    Returns
    -------
    fname : str
        A unique filename.
    """
    
    t1 = dt.utcnow()

    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]
    dtime_string = month + '-' + day + '_at_' + hour + minute + 'utc'
    
    
    fname = os.path.join(fpath, 
                         'submissions_list_by' + how + '_' 
                         + dtime_string + '.p'
                        )
    
    return fname
    



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