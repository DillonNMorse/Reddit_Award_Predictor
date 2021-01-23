# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:31 2021

@author: Dillo
"""

from datetime import datetime as dt
import pickle
import os
import json

import boto3

#from retrieve_comments import get_toplevel_comment_info





def submission_features(event, context):
    
    inputs_dict = json.loads(event['body'])
    
    fname = inputs_dict['fname']
    working_directory = inputs_dict['working_directory']
    bucket_name = inputs_dict['bucket_name']
                    
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
    s3 = boto3.resource('s3')
    submissions_fpath = os.path.join(working_directory, 'submissions_' + fname)
    data_obj = s3.Object(bucket_name, submissions_fpath)
    submissions_pickle = data_obj.get()['Body'].read()
    
    
    
    
    """
    Begin function.
    """
    # Load submission data from file.
    submissions = pickle.loads(submissions_pickle)
    
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
    features_fpath = os.path.join(working_directory, 'posts_data_' + fname)
    features_data_obj = pickle.dumps(features)
    s3.Object(bucket_name, features_fpath).put(Body = features_data_obj)    
    
    return  {
            'statusCode': 200,
            }



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