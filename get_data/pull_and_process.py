# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 11:58:16 2020

@author: Dillo
"""


import praw

from extract_features import submission_features


def pull_and_process(reddit_auth_file, subreddit_list, num_posts, 
                     num_top_comments = 15, how = 'new'
                    ):
    """
    Makes API call (in batches of 100 submissions) and passes each batch
    of submissions on to have features extracted.

    Parameters
    ----------
    reddit_auth_file : str
        Filename of .txt file containing Reddit API keys.
    subreddit_list : list
        List of strings indicating the subreddits to be included in the 
        analysis.
    num_posts : int
        The total number of submissions to be retrieved.
    num_top_comments : int, optional
        How many of the submission's comments (sorted by comments' upvotes,
        descending) should be used in the analysis. Younger submissions may not
        yet have many comments. The default is 15.
    how : str, optional
        How the submissions are to be sorted within Reddit. The default is 
        'rising'.

    Returns
    -------
    all_submission_features : TYPE
        DESCRIPTION.

    """
    
    auth = get_auth(reddit_auth_file)
    
    reddit = praw.Reddit(client_id = auth.client_id,
                         client_secret = auth.client_secret,
                         user_agent = auth.user_agent,
                         )

    reddit_subs_to_pull = subreddit_list_to_string(subreddit_list)
    subreddit_instance = getattr(reddit.subreddit(reddit_subs_to_pull),
                                 how
                                )(limit = num_posts)
                         
    all_submission_features = {}
    submission_batch = []
    for subm_num, subm in enumerate(subreddit_instance):
        
        submission_batch.append(subm)
        
        if ((subm_num + 1)%100 == 0) | (subm_num == num_posts - 1):
            batch_features = submission_features(auth,
                                                 submission_batch,
                                                 num_top_comments,
                                                )
            submission_batch = []
            all_submission_features.update(batch_features)
      
    return all_submission_features




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
    auth : pull_and_process.auth
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




















