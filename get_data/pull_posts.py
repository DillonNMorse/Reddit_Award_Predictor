# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:44:35 2021

@author: Dillo
"""

import os
from datetime import datetime as dt
#import pickle
import json

import praw
import boto3


def pull_posts(event, context):
    
    num_posts = event['num_posts']
    num_top_comments = event['num_top_comments']
    how = event['how']
    
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
    
    
    

    """
    Variables that define the locations of resources within s3 buckets. They
    are included in the Lambda function as environmental variables.
    Parameters
    ----------
    bucket_name : str
        The name of the bucket within AWS s3 that the project will be working 
        out of.
    auth_filename : str
        The filename (including extension) of the .txt file containing Reddit
        API keys. See sample_auth.txt for required layout.
    subreddits_filename : str
        The filename (including extension) where the list of subreddits to be
        included is stored. See subreddits.txt
    working_directory : str 
        The name of the directory within the AWS s3 bucket where intermediate
        data will be stored for processing.
    staging_directory: str
        The name of the directory within the AWS s3 bucket where data will be
        stored after gilds have been fetched, before appending in batches to 
        the database.
    """
    bucket_name = os.environ['bucket_name']
    auth_filename = os.environ['auth_filename']
    subreddits_filename = os.environ['subreddits_filename']
    #working_directory = os.environ['working_directory']
    staging_directory = os.environ['staging_directory']
    
    
    
    """ 
    Set up calls to AWS S3 bucket that contains the files for Reddit API 
    authorization and the set of subreddits to pull for analysis.
    """    
    s3 = boto3.resource('s3')
    
    auth_obj = s3.Object(bucket_name, auth_filename)
    auth_file = auth_obj.get()
    
    subreddits_obj = s3.Object(bucket_name, subreddits_filename)
    subreddits_file = subreddits_obj.get()

    

    """
    Begin function.
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
    #fpath = os.path.join(working_directory, 'submissions_' + fname)
    #submissions_pickle = pickle.dumps(submissions)
    #s3.Object(bucket_name, fpath).put(Body = posts_data_obj)
    
    
    
    posts_features = submission_features(submissions)
    
    
    return_dict = {'fname': fname,
                   #'working_directory': working_directory,
                   #'IDs': submissions_IDs,
                   'num_top_comments': num_top_comments,
                   'auth_filename': auth_filename,
                   'bucket_name': bucket_name,
                   'staging_directory': staging_directory,
                   #'submissions': submissions#_pickle,
                   'posts_features': posts_features
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
    
    
    fname = os.path.join('sortedby_' + how + '_'+ dtime_string + '.pkl')
    
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
    
    return subreddits_file['Body'].read().decode('utf-8').split('\r')




def submission_features(submissions):
    
    #inputs_dict = json.loads(event['body'])
    
    #fname = inputs_dict['fname']
    #working_directory = inputs_dict['working_directory']
    #bucket_name = inputs_dict['bucket_name']
    #submissions = inputs_dict['submissions']
                    
    """
    Retrieve relevent feature data from a batch of Reddit posts. The API has
    already been called for the post, so further API calls will only be invoked
    when retrieving additional data about the posts' comments.

    Parameters
    ----------

    working_directory: str
        String naming the directory within the AWS s3 bucket where data is 
        stored.
    fname: str
        Unique string used to build filename.
    bucket_name : str
        The name of the AWS s3 bucket containing data.

    Returns
    -------
    batch_features : dict
        Dictionary with keys given by individual Reddit posts submitted, the
        corresponding values are dictionaries containing the features extracted
        from that post.

    """
    
    
    
    
    """ 
    Set up call to AWS S3 bucket that contains the pulled Reddit post data.
    """
    #s3 = boto3.resource('s3')
    #submissions_fpath = os.path.join(working_directory, 'submissions_' + fname)
    #data_obj = s3.Object(bucket_name, submissions_fpath)
    #submissions_pickle = data_obj.get()['Body'].read()

    
    
    
    """
    Begin function.
    """
    # Load submission data from file.
    #submissions = pickle.loads(submissions_pickle)
    
    # Dictionary containing names for API featues.
    api_feat = api_feature_names()
    
    # Initialize dictionary to hold features for Reddit posts
    features = {}
    # Iterate over each submission. They have all been pulled from the API,
    #   they will now just be processed.
    for subm in submissions:
        
        features[subm.id] = {}
            
        for feat_name in api_feat:
            features[subm.id][feat_name] = subm.__dict__[api_feat[feat_name]]
    
        # Extract author and subreddit names as strings
        try:
            features[subm.id]['Author'] = features[subm.id]['Author'].name
        except AttributeError:
            features[subm.id]['Author'] = None
        try:
            features[subm.id]['Subreddit'] = (features[subm.id]['Subreddit']
                                              ).display_name
        except AttributeError:
            features[subm.id]['Subreddit'] = None
        
        features[subm.id]['Created utc'] = subm.created_utc
        # Convert UTC timestamp to time of day (in minutes since beginning of 
        #   UTC day)
        dtime_posted = dt.utcfromtimestamp(features[subm.id]['Post time'])
        features[subm.id]['Post time'] = (dtime_posted.hour*60 
                                          + dtime_posted.minute
                                         )
        
        # Calculate age of the post (in minutes)
        features[subm.id]['Post age'] = (dt.utcnow() 
                                         - dtime_posted).total_seconds()/60
        
        # Calculate upvotes per minute of age and comments per minute of age
        features[subm.id]['Upvote rate'] = (features[subm.id]['Upvotes']
                                            /features[subm.id]['Post age']
                                           )
        features[subm.id]['Comment rate'] = (features[subm.id]['Comments']
                                             /features[subm.id]['Post age']
                                            )
      
    # Save all post features to s3 bucket.
    #features_fpath = os.path.join(working_directory, 'posts_data_' + fname)
    #posts_features_pickle = pickle.dumps(features)
    #s3.Object(bucket_name, features_fpath).put(Body = features_data_obj) 
    
    return features
    
# =============================================================================
#     return_dict = {'posts_features': features}
#     
#     return  {
#             'statusCode': 200,
#             'body': json.dumps(return_dict)
#             }
# =============================================================================



def api_feature_names():
    # List of potentially informative features available from the API
    api_feat = {'Title': 'title',
                'Author': 'author',
                'ID': 'id',
                'Gilded': 'gilded',
                'Gildings': 'gildings',
                'Upvotes': 'ups',
                'Upvote ratio': 'upvote_ratio',
                'Post time': 'created_utc',
                'Views': 'view_count',
                'Discussion type': 'discussion_type',
                'Distinguished': 'distinguished',
                'Contest mode': 'contest_mode',
                'Content categories': 'content_categories',
                'Edited': 'edited',
                'Hidden': 'hidden',
                'Crosspostable': 'is_crosspostable',
                'Crossposts': 'num_crossposts',
                'Meta': 'is_meta',
                'OC': 'is_original_content',
                'Reddit media': 'is_reddit_media_domain',
                'Robot indexable': 'is_robot_indexable',
                'Selfpost': 'is_self',
                'Video': 'is_video',
                'Likes': 'likes',
                'Comments': 'num_comments',
                'Adult content': 'over_18',
                'Subreddit': 'subreddit',
               }
    return api_feat
 