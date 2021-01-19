# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:31 2021

@author: Dillo
"""

from datetime import datetime as dt
import pickle
import os

from retrieve_comments import get_toplevel_comment_info




def submission_features(auth,
                        subms_fpath, 
                        directory,
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
    submissions : list
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
    
    # Load submission data from file.
    submissions = pickle.load(open(subms_fpath, 'rb'))
    
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
      
    # Save post features to disc.
    posts_dtime = subms_fpath[27:]
    posts_fpath = os.path.join(directory, 'posts_data_' + posts_dtime)
    pickle.dump(features, open(posts_fpath, 'wb'))    
    
    return posts_fpath



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