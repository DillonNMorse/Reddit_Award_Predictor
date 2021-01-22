# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:56:03 2021

@author: Dillo
"""

import pickle
import os
import json

import boto3

""" 
Set up call to AWS S3 bucket that contains the prepared features.
"""

bucket_name = 'dnmorse-reddit-predict-private-directories'
auth_filename = 'auth.txt'

s3 = boto3.resource('s3')

def combine_reddit_data(event, context):
    
    inputs_dict = json.loads(event[1]['body'])

    
    fname = inputs_dict['fname']
    working_directory = inputs_dict['working_directory']

    # Load submission data from file.
    posts_feats_fpath = os.path.join(working_directory, 'posts_data_' + fname)
    comments_fpath = os.path.join(working_directory, 'comments_data_' + fname)
    
    posts_obj    = s3.Object(bucket_name, posts_feats_fpath)
    comments_obj = s3.Object(bucket_name, comments_fpath)
    
    posts_pickle    = posts_obj.get()['Body'].read()
    comments_pickle = comments_obj.get()['Body'].read()
    
    posts = pickle.loads(posts_pickle)
    comments = pickle.loads(comments_pickle)
    
    # Combine the two dicts, removing any posts already gilded when pulled.
    combined = posts.copy()
    for ID in combined:
        num_gilds = count_gildings(combined[ID]['Gildings'])
        if num_gilds > 0:
            del combined[ID]
        else:
            combined[ID].update(comments[ID])
    
        
    # Save all features to s3 bucket.
    combined_fpath = os.path.join(working_directory, 'all_features_' + fname)
    combined_data_obj = pickle.dumps(combined)
    s3.Object(bucket_name, combined_fpath).put(Body = combined_data_obj)     
    
    
    return {
            'statusCode': 200,
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