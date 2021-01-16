# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:31 2021

@author: Dillo
"""

from datetime import datetime as dt

from retrieve_comments import get_toplevel_comment_info




def submission_features(auth, submission_batch, 
                        num_top_comments = 15,
                       ):
    """
    Retrieve relevent feature data from a batch of Reddit posts. The API has
    already been called for the post, so further API calls will only be invoked
    when retrieving additional data about the posts' comments.

    Parameters
    ----------
    auth_dict : dict
        Dictionary containing authorization keys for Reddit API. To be passed
        on for retrieval of comment data.
    submission_batch : list
        List of reddit submissions to be processed.
    num_top_comments : int, optional
        The number of top-level comments to include in the analysis. The 
        default is 15.
    api_feat : dict, optional
        Dictionary containing mappings between Reddit submission feature names
        and the names to be used here. The default is api_feat.

    Returns
    -------
    batch_features : dict
        Dictionary with keys given by individual Reddit posts submitted, the
        corresponding values are dictionaries containing the features extracted
        from that post.

    """
    
    api_feat = api_feature_names()
    
    # Initialize dictionary to hold features for whole batch of Reddit submissions
    batch_features = {}
    # Iterate over each submission in the batch. The entire batch has already
    #    been pulled by the API, so they will now just be processed.
    for subm in submission_batch:
        # Iterate through desired features to build a dictionary containing feature values for this submission
        features = {}
        for feat_name in api_feat:
            features[feat_name] = subm.__dict__[api_feat[feat_name]]
    
        # Extract author and subreddit names as strings
        try:
            features['Author'] = features['Author'].name
        except AttributeError:
            features['Author'] = None
        try:
            features['Subreddit'] = features['Subreddit'].display_name
        except AttributeError:
            features['Subreddit'] = None
        
        # Convert UTC timestamp to time of day (in minutes since beginning of UTC day)
        dtime_posted = dt.utcfromtimestamp(features['Post time'])
        features['Post time'] = dtime_posted.hour*60 + dtime_posted.minute
        
        # Calculate age of the post (in minutes)
        features['Post age'] = (dt.utcnow() - dtime_posted).total_seconds()/60
        
        # Calculate upvotes per minute of age and comments per minute of age
        features['Upvote rate'] = features['Upvotes']/features['Post age']
        features['Comment rate'] = features['Comments']/features['Post age']
        
        
        # Call function to process comment data for single submissions
        (Avg_up_rate, Std_up_rate, gild_rate, distinguished_rate,
            op_comment_rate, premium_auth_rate,
        ) = get_toplevel_comment_info(auth, subm, num_top_comments)
    
        # Assign to dictionary
        features['Avg top comments up rate'] = Avg_up_rate
        features['Std top comments up rate'] = Std_up_rate
        features['Gilded rate'] = gild_rate
        features['Distinguished rate'] = distinguished_rate
        features['Op comment rate'] = op_comment_rate
        features['Premium author rate'] = premium_auth_rate

        # Append this submission's feature to the set of all features for the batch
        batch_features[subm] = features
    
    return batch_features



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