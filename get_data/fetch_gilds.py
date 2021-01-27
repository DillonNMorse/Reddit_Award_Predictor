# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 15:57:01 2021

@author: Dillo
"""

import os
import json
import pickle

import boto3
import praw


def fetch_gilds(event, context):

    inputs_dict = json.loads(event['body'])

    fname = inputs_dict['fname']
    bucket_name = inputs_dict['bucket_name']
    #working_directory = inputs_dict['working_directory']
    auth_filename = inputs_dict['auth_filename']
    staging_directory = inputs_dict['staging_directory']
    all_features = inputs_dict['all_features']
    
    """ 
    Set up call to AWS S3 bucket that contains the file for Reddit API 
    authorization.
    """
    s3 = boto3.resource('s3')
    
    # Load Reddit auth data
    auth_obj = s3.Object(bucket_name, auth_filename)
    auth_file = auth_obj.get()
    
# =============================================================================
#     # Load combined Reddit posts data
#     submissions_fpath = os.path.join(working_directory,
#                                      'all_features_' + fname
#                                     )
#     data_obj = s3.Object(bucket_name, submissions_fpath)
#     submissions_pickle = data_obj.get()['Body'].read()    
# =============================================================================
    
    
    
    
    """
    Begin function.
    """
    # Load submission data from file.
    #submissions = pickle.loads(all_features_pickle)
    
    # Pull Reddit API keys from file
    auth = get_auth(auth_file)
    
    # Instantiate a Reddit API call header
    reddit = praw.Reddit(client_id = auth.client_id,
                         client_secret = auth.client_secret,
                         user_agent = auth.user_agent,
                         )    
    
    # Get IDs of all Reddit posts
    IDs = list(all_features.keys())
    
    # Build list of fullnames and get data for all - will automatically call
    #   Reddit API in batches to optimize speed.
    fullname_list = ['t3_' + ID for ID in IDs]
    posts = reddit.info(fullname_list) 

    # Iterate over Reddit posts and extract Gilds data    
    new_data = get_gilds_and_upvotes(posts)
 
    # Add gilding data to submissions data
    data_with_gilds = all_features.copy()
    for ID in data_with_gilds:
        data_with_gilds[ID].update(new_data[ID])
    
    # Save all features to s3 bucket.
    gilded_data_fpath = os.path.join(staging_directory, 'gilded_data_' + fname)
    gilded_data_obj = pickle.dumps(data_with_gilds)
    s3.Object(bucket_name, gilded_data_fpath).put(Body = gilded_data_obj)     
    
    return {
            'statusCode': 200,
           }




def get_gilds_and_upvotes(posts):
    new_data = {}
    for post in posts:      
        post_gilds = post.gildings      
        try:
            silvers = post_gilds['gid_1']
        except KeyError:
            silvers = 0
        try:
            golds = post_gilds['gid_2']
        except KeyError:
            golds = 0
        try:
            platinums = post_gilds['gid_3']
        except KeyError:
            platinums = 0
        
        new_data[post.id] = {}
        new_data[post.id]['Silver awarded'] = silvers
        new_data[post.id]['Gold awarded'] = golds
        new_data[post.id]['Platinum awarded'] = platinums
        
        new_data[post.id]['Final upvotes'] = post.ups
        new_data[post.id]['Final num comments'] = post.num_comments
    
    return new_data




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