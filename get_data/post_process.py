# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:56:03 2021

@author: Dillo
"""

import pickle
import os
import json

import boto3


def combine_reddit_data(event, context):
    
    inputs_dict = json.loads(event[1]['body'])

    fname = inputs_dict['fname']
    working_directory = inputs_dict['working_directory']
    bucket_name = inputs_dict['bucket_name']
    auth_filename = inputs_dict['auth_filename']
    staging_directory = inputs_dict['staging_directory']
    
    """
    Combine the features extracted from the Reddit posts and from the top
    comments of each Reddit post. Remove any posts which were already gilded 
    at the time of scraping the posts. Output as pickle file in s3 bucket, will 
    eventually be ammended to include a y/n indicator as to whether the post
    was guilded within 24 hours or not.

    Parameters
    ----------
    fname : str
        The unique string identifying the Reddit posts pulled.
    working_directory : str
        The directory within the s3 bucket which holds intermediate data.
    bucket_name : str
        The name of the AWS s3 bucket containing data.

    Returns
    -------
    None
        Dictionary saved to AWS s3 bucket in working directory.

    """

    
    
    
    """ 
    Set up call to AWS S3 bucket that contains the prepared features.
    """
    s3 = boto3.resource('s3')

    posts_feats_fpath = os.path.join(working_directory, 'posts_data_' + fname)
    comments_fpath = os.path.join(working_directory, 'comments_data_' + fname)
    
    posts_obj    = s3.Object(bucket_name, posts_feats_fpath)
    comments_obj = s3.Object(bucket_name, comments_fpath)
    
    posts_pickle    = posts_obj.get()['Body'].read()
    comments_pickle = comments_obj.get()['Body'].read()
    
    
    
    
    """
    Begin function.
    """
    # Load both sets of data from pickle files.
    posts = pickle.loads(posts_pickle)
    comments = pickle.loads(comments_pickle)
    
    # Combine the two dicts, ignore any posts already gilded when pulled.
    combined = {}
    for ID in posts:
        num_gilds = count_gildings(posts[ID]['Gildings'])
        if num_gilds == 0:
            combined[ID] = posts[ID]
            combined[ID].update(comments[ID])

        
    # Save all features to s3 bucket.
    combined_fpath = os.path.join(working_directory, 'all_features_' + fname)
    combined_data_obj = pickle.dumps(combined)
    s3.Object(bucket_name, combined_fpath).put(Body = combined_data_obj)  
    
    return_dict = {'auth_filename': auth_filename,
                   'fname': fname,
                   'working_directory': working_directory,
                   'bucket_name': bucket_name,
                   'staging_directory': staging_directory
                  }
    
    
    return {
            'statusCode': 200,
            'body': json.dumps(return_dict)
           }




def count_gildings(gild_dict):
    
    # Count number of gold plus platinums awarded
    try:
        silvers = gild_dict['gid_1']
    except KeyError:
        silvers = 0
    try:
        golds = gild_dict['gid_2']
    except KeyError:
        golds = 0
    try:
        platinums = gild_dict['gid_3']
    except KeyError:
        platinums = 0
        
    return golds + platinums