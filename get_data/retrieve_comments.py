# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 17:13:31 2021

@author: Dillo
"""


from datetime import datetime as dt
import json
import pickle
import os

import boto3
import requests


def  process_all_posts_comments(event, context):
    
    inputs_dict = json.loads(event['body'])

    fname = inputs_dict['fname']
    subm_IDs =  inputs_dict['IDs']
    working_directory = inputs_dict['working_directory']
    num_top_comments = inputs_dict['num_top_comments']
    bucket_name = inputs_dict['bucket_name']
    auth_filename = inputs_dict['auth_filename']
    staging_directory = inputs_dict['staging_directory']
    
    """
    Wrapper for get_toplevel_comment_info, iterates through submission ID's
    and packages processed comment data in to a dictionary, then saved to
    AWS s3 bucket.

    Parameters
    ----------
    working_directory: str
        String naming the directory within the AWS s3 bucket where data is 
        stored.
    fname: str
        Unique string used to build filename.
    subm_IDs: list
        List of tuples, length = number of posts pulled. First tuple entry is 
        the ID of post, second tuple entry is the subreddit its from. 
     num_top_comments: int
         The number of toplevel comments, sorted in Reddit by upovtes,
         descending, to use per Reddit post.
    bucket_name : str
        The name of the AWS s3 bucket containing data.
    auth_filename : str
        The name of the txt file containing Reddit API keys, stored within the 
        root of the bucket named above. 

    Returns
    -------
    dict
        Dictionary containing comment features for all posts. Keys are post
        ID, values are dictionaries containing features for corresponding post.
    """
    
    
    
    
    """ 
    Set up call to AWS S3 bucket that contains the file for Reddit API 
    authorization.
    """
    s3 = boto3.resource('s3')
    
    auth_obj = s3.Object(bucket_name, auth_filename)
    auth_file = auth_obj.get()
    
    
    
    """
    Begin function.
    """
    # Pull Reddit API keys from file
    auth = get_auth(auth_file)
    
    comments_dict = {}
    for subm in subm_IDs:
        submission_id = subm[0]
        subm_subreddit = subm[1] 
        comment_data = get_toplevel_comment_info(auth,
                                                 submission_id,
                                                 subm_subreddit,
                                                 num_top_comments
                                                )
        
        # Store post data to a dictionary
        comments_dict[submission_id] = {}
        comments_dict[submission_id]['avg_up_rate'] = comment_data[0]
        comments_dict[submission_id]['std_up_rate'] = comment_data[1]
        comments_dict[submission_id]['gild_rate'] = comment_data[2]
        comments_dict[submission_id]['distinguished_rate'] = comment_data[3]
        comments_dict[submission_id]['op_comment_rate'] = comment_data[4]
        comments_dict[submission_id]['premium_auth_rate'] = comment_data[5]
        

    # Save all comment features to s3 bucket.
    comments_fpath = os.path.join(working_directory, 'comments_data_' + fname)
    comments_data_obj = pickle.dumps(comments_dict)
    s3.Object(bucket_name, comments_fpath).put(Body = comments_data_obj) 
    
    return_dict = {'fname': fname,
                   'working_directory': working_directory,
                   'bucket_name': bucket_name,
                   'staging_directory': staging_directory,
                   'auth_filename': auth_filename
                  }
    
    return {
            'statusCode': 200,
            'body': json.dumps(return_dict)
            }




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
    data : dict
        Dictionary containing all data about a particular comment.
    feature : str
        String indicating the particular feature to be extracted.

    Returns
    -------
    value : (str or int or float)
        The value for the feature of interest for the particular comment.

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