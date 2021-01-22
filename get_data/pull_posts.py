# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:44:35 2021

@author: Dillo
"""

import os
from datetime import datetime as dt
import pickle
import json

import praw
import boto3


""" 
Set up calls to AWS S3 bucket that contains the files for Reddit API 
authorization and the set of subreddits to pull for analysis.
"""

bucket_name = 'dnmorse-reddit-predict-private-directories'
auth_filename = 'auth.txt'
subreddits_filename = 'subreddits.txt'

s3 = boto3.resource('s3')

auth_obj = s3.Object(bucket_name, auth_filename)
auth_file = auth_obj.get()

subreddits_obj = s3.Object(bucket_name, subreddits_filename)
subreddits_file = subreddits_obj.get()




working_directory = './work'

def pull_posts(event, context):
    
    num_posts = event['num_posts']
    num_top_comments = event['num_top_comments']
    how = event['how']
    working_directory = event['working_directory']
    
    """
    Makes API call, saves all submissions to disc for later processing.

    Parameters
    ----------
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
        The path of the directory to save the submissions.

    Returns
    -------
    fname: str
        Unique string used to build filename.
    IDs: list
        List of tuples, length = number of posts pulled. First tuple entry is 
        the ID of post, second tuple entry is the subreddit its from.
    working_directory: str
        The directory where intermediate files will be saved.
    num_top_comments: int
        The number of toplevel comments to use for analysis on each post.
    """
    
    # Pull Reddit API keys from file
    auth = get_auth(auth_file)
        
    # Instantiate a Reddit API call header
    reddit = praw.Reddit(client_id = auth.client_id,
                         client_secret = auth.client_secret,
                         user_agent = auth.user_agent,
                         )
    
    # Convert subreddits file to a string compatible with Reddit API
    subreddit_list = get_subreddit_list(subreddits_file)
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
    fname = build_working_filename(how)
    fpath = os.path.join(working_directory, 'submissions_' + fname)
    posts_data_obj = pickle.dumps(submissions)
    s3.Object(bucket_name, fpath).put(Body = posts_data_obj)
    
    return_dict = {'fname': fname,
                   'working_directory': working_directory,
                   'IDs': submissions_IDs,
                   'num_top_comments': num_top_comments,
                   #'auth': auth,
                   }
    return {
            'statusCode': 200,
            'body': json.dumps(return_dict),
           }




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
        lines = reddit_auth_file['Body'].read().decode('utf-8').split('\r')
        #lines = open(reddit_auth_file, "r") # When passing a filepath
        for line in lines:
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
        #lines.close() # When passing a filepath            




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




def build_working_filename(how):
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
    
    
    fname = os.path.join('sortedby_' + how + '_'+ dtime_string + '.p')
    
    return fname
    



def get_subreddit_list(subreddits_file):
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
    
#    with open(subreddits_file, "r") as f: # When passing a filepath 
#        subreddit_list = f.read().split(',\n')
    
    return subreddits_file['Body'].read().decode('utf-8').split('\r')
#    return subreddit_list # When passing a filepath 