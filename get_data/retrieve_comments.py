# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:31 2021

@author: Dillo
"""

import requests
from datetime import datetime as dt
import json
import pickle
import os



def  process_all_posts_comments(subms_fpath, subm_IDs, auth, directory):
    
    #Wrapper for get_toplevel_comment_info, iterates through submission ID's 
    comments_dict = {}
    for subm in subm_IDs:
        submission_id = subm[0]
        subm_subreddit = subm[1] 
        comment_data = get_toplevel_comment_info(auth,
                                                 submission_id,
                                                 subm_subreddit
                                                )
        
        # Store post data to a dictionary
        comments_dict[submission_id] = {}
        comments_dict[submission_id]['avg_up_rate'] = comment_data[0]
        comments_dict[submission_id]['std_up_rate'] = comment_data[1]
        comments_dict[submission_id]['gild_rate'] = comment_data[2]
        comments_dict[submission_id]['distinguished_rate'] = comment_data[3]
        comments_dict[submission_id]['op_comment_rate'] = comment_data[4]
        comments_dict[submission_id]['premium_auth_rate'] = comment_data[5]
        
    # Save to disc
    comms_dtime = subms_fpath[27:]
    comms_fpath = os.path.join(directory, 'comment_data_' + comms_dtime)
    pickle.dump(comments_dict, open(comms_fpath, 'wb'))
    
    return comms_fpath




def get_toplevel_comment_info(auth,
                              submission_id,
                              subm_subreddit,
                              num_top_comments = 15
                              ):
    """
    Given a single submission, process its top comments to build relevant
    features for analysis. 

    Parameters
    ----------
    auth_dict : dict
        Dictionary which contains authorization info for accesing Reddit API.
    submission : [reddit submission]
        The Reddit post for which comment info is to be extracted.
    num_top_comments : int, optional
        How many comments, sorted by upvotes descending, will be used to
        perform the analysis. The default is 15.

    Returns
    -------
    Avg_up_rate : float
        Average the number of upvotes across comments.
    Std_up_rate : float
        Standard Deviation of number of upvotes across comments.
    gild_rate : float
        The percentage of comments which received one or more gildings.
    distinguished_rate : float
        The percentage of comments made by a distinguished user (admin,
        moderator, etc.).
    op_comment_rate : float
        The percentage of comments made by the original poster.
    premium_auth_rate : float
        The percentage of comments made by users in the premium category.

    """

    
    # Set up authentication for Reddit API
    #   Due to some rate-limiting, cannot practically use PRAW for comments
    base_url = str('https://www.reddit.com/r/')
    authorization = requests.auth.HTTPBasicAuth(auth.client_id,
                                                auth.client_secret,
                                                )
    headers = {'user-agent': auth.user_agent}
    
    #Call the API with these params
    params = {'context': 1,
              'depth': 1,
              'limit': num_top_comments,
              'showedits': 0,
              'showmore': 0,
              'sort': 'top',
              'threaded': 0,
              'truncate': 50,
              }
    #submission_id = str(submission.id)
    #submission_sub = str(submission.subreddit)
    url = base_url + subm_subreddit + '/comments/' + submission_id + '.json'

    # Call API
    r = requests.get(url,
                     params = params,
                     auth = authorization,
                     headers = headers,
                     )
    
    # Convert json response to dictionary
    comments_dict = json.loads(r.text)[1]['data']['children']
    num_comments = len(comments_dict)


    created =   [comment_value(comment['data'], 'created_utc')
                 for comment in comments_dict
                ]
    upvotes =   [comment_value(comment['data'], 'ups')
                 for comment in comments_dict
                ]
    gilded  =   [comment_value(comment['data'], 'gilded')
                 for comment in comments_dict
                ]
    disting =   [comment_value(comment['data'], 'distinguished')
                 for comment in comments_dict
                ]
    op_comm =   [comment_value(comment['data'], 'is_submitter')
                 for comment in comments_dict
                ]
    auth_prem = [comment_value(comment['data'], 'author_premium')
                 for comment in comments_dict
                ] 
    
    # Use the above to calculate summary info about comment section for submission
    current_dtime = dt.utcnow()
    age = [(current_dtime - dt.utcfromtimestamp(time)).total_seconds() 
           for time in created
          ]
    
    up_rate = [ups/time for ups,time in zip(upvotes, age)]
    if num_comments == 0:
        Avg_up_rate = 0
        Std_up_rate = 0
        gild_rate = 0
        distinguished_rate = 0
        op_comment_rate = 0
        premium_auth_rate = 0
    else:
        Avg_up_rate = sum(up_rate)/num_comments
        Std_up_rate = sum([(up - Avg_up_rate)**2 for up in up_rate])/num_comments        
        gild_rate          = sum(gilded)/num_comments
        distinguished_rate = sum(disting)/num_comments
        op_comment_rate    = sum(op_comm)/num_comments
        premium_auth_rate  = sum(auth_prem)/num_comments
        
    
    # There is an opportunity to create more features out of the comments. These could include:
    #    explore the success of comments/replies made by the submitter of the original post
    #    look at the distribution of comment upvotes (or of comment replies)
    #    look at the average controversiality among comments or replies to comments
    #    calculate the rate of gildings among comments and/or comment replies
    
    return (Avg_up_rate, Std_up_rate, gild_rate, distinguished_rate,
            op_comment_rate, premium_auth_rate,
           )




def comment_value(data, feature):
    """
    Retrieve information about a specific feature from the comment data from a 
    Reddit post. Handles possible exceptions due to lack of consistent 
    formatting and/or data included in API calls. Also does some basic encoding
    of categorical variables for the feature: 'distinguished'.

    Parameters
    ----------
    data : TYPE
        DESCRIPTION.
    feature : TYPE
        DESCRIPTION.

    Returns
    -------
    value : TYPE
        DESCRIPTION.

    """
    
    try:
        value = data[feature]
        if value == None:
            value = 0
        elif (feature == 'distinguished') & (value == 'moderator'):
            value = 1
        elif (feature == 'distinguished') & (value == 'admin'):
            value = 1            
    except KeyError:
        value = 0   
    return value