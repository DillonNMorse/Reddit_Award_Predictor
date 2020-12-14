# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 11:58:16 2020

@author: Dillo
"""


import praw
import pprint
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import requests
import numpy as np
import json

# Need to create a script that pulls and processes posts in batches. 

# Start by altering the processing scripts so that they can take batches of
# submissions instead of single submissions.

# First need to know what format the batches will come in. Lets start there.

client_id = 'h8BBe0NNslxi8g'
client_secret = 'gd2EfD_bd9njZI9zngbiD1WhMJo8lA'
user_agent = 'Chrome:AwardPredictor:v0.0.1 (by /u/drdnm)'




# =============================================================================
# reddit = praw.Reddit(client_id = client_id, 
#                      client_secret = client_secret, 
#                      user_agent = user_agent)
# 
# 
# 
# 
# multi = reddit.subreddit('aww+history+askreddit')
# multi_hot = multi.hot(limit = 225)
# =============================================================================






def pull_and_process(reddit_auth, subreddit_list, num_posts, 
                     num_top_comments = 15, how = 'rising'
                    ):
    
    reddit = praw.Reddit(client_id = client_id,
                         client_secret = client_secret,
                         user_agent = user_agent,
                         )
    
    reddit_subs_to_pull = subreddit_list_to_string(subreddit_list)
    subreddit_instance = (reddit.subreddit(reddit_subs_to_pull)
                                .rising(limit = num_posts)
                         )
    
    all_submission_features = {}
    submission_batch = []
    for subm_num, subm in enumerate(subreddit_instance):
        
        submission_batch.append(subm)
        
        if ((subm_num + 1)%100 == 0) | (subm_num == num_posts - 1):
            batch_features = submission_features(submission_batch, num_top_comments)
            submission_batch = []
            all_submission_features.update(batch_features)
      
    return all_submission_features




def subreddit_list_to_string(subreddit_list):
    subreddit_multi_name = ''
    for subreddit_name in subreddit_list:
        subreddit_multi_name += '+' + subreddit_name.strip()
    
    return subreddit_multi_name[1:]




def get_toplevel_comment_info(submission, num_top_comments = 15):
    
    # Given a single submission, process its top 'num_top_comments' comments
    
    # Set up authentication for Reddit API
    #   Due to some rate-limiting, cannot practically use PRAW for comments
    base_url = str('https://www.reddit.com/r/')
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    headers = {'user-agent': user_agent}
    
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
    current_tstmp = datetime.datetime.now()
    submission_id = str(submission.id)
    submission_sub = str(submission.subreddit)
    url = base_url + submission_sub + '/comments/' + submission_id + '.json'

    # Call API
    r = requests.get(url,
                     params = params,
                     auth = auth,
                     headers = headers,
                     )
    
    # Convert json response to dictionary
    comments_dict = json.loads(r.text)[1]['data']['children']

    # Pull information from the set of fetched comments
    #    Some are not currently being used, but could be useful in future
    age = [(current_tstmp - 
           datetime.datetime.fromtimestamp(comment['data']['created_utc'])
           ).total_seconds()
           for comment in comments_dict
           ]
    upvotes   = [comment['data']['ups'] for comment in comments_dict]
   #downvotes = [comment['data']['downs'] for comment in comments_dict]
    gilded    = [comment['data']['gilded'] for comment in comments_dict]
   #gildings  = [comment['data']['gildings'] for comment in comments_dict]
    disting   = [0 if comment['data']['distinguished'] is None else
                 comment['data']['distinguished'] for comment in comments_dict
                ]
    
   #edited    = [comment['data']['edited'] for comment in comments_dict]
   #controv   = [comment['data']['controversiality'] for comment in comments_dict]
    op_comm   = [comment['data']['is_submitter'] for comment in comments_dict]
    auth_prem = [comment['data']['author_premium']  for comment in comments_dict]
    
# =============================================================================
# =============================================================================
# #    auth premium sometimes throws KeyError where 'author_premium' is apparently missing
# #         need to write custom function with some try-except statements
# =============================================================================
# =============================================================================
    
    # Use the above to calculate summary info about comment section for submission
    up_rate = [ups/time for ups,time in zip(upvotes, age)]
    
    if len(up_rate) == 0:
        Avg_up_rate = None
        Std_up_rate = None
    else:
        Avg_up_rate = sum(up_rate)/len(up_rate)
        Std_up_rate = sum([(up - Avg_up_rate)**2 for up in up_rate])/len(up_rate)
        
    num_gildings = sum(gilded)
    num_disting = sum(disting)
    num_op_comments = sum(op_comm)
    num_premiums = sum(auth_prem)
        
    
    # There is an opportunity to create more features out of the comments. These could include:
    #    explore the success of comments/replies made by the submitter of the original post
    #    look at the distribution of comment upvotes (or of comment replies)
    #    look at the average controversiality among comments or replies to comments
    #    calculate the rate of gildings among comments and/or comment replies
    
    return Avg_up_rate, Std_up_rate, num_gildings, num_disting, num_op_comments, num_premiums



def process_comments_for_batch(submission_batch, num_top_comments = 15):
    # Set up dictionary to hold data, indexed by individual Reddit submissions
    batch_comment_summaries = {}
    
    # Iterate through all submissions in the batch
    for submission in submission_batch:
        # Create a dictionary to hold data for each individual summary
        batch_comment_summaries[submission] = {}
        
        # Call function to process comment data for single submissions
        (avg_up, std_up, gild,
         disting, op_comms, premiums
        ) = get_toplevel_comment_info(submission, num_top_comments)
    
        # Assign to dictionary
        batch_comment_summaries[submission]['Avg_up_rate'] = avg_up
        batch_comment_summaries[submission]['Std_up_rate'] = std_up
        batch_comment_summaries[submission]['Num_gildings'] = gild
        batch_comment_summaries[submission]['Num_distinguished'] = disting
        batch_comment_summaries[submission]['Num_Op_Comments'] = op_comms
        batch_comment_summaries[submission]['Num_premium_auths'] = premiums
        
        
    return batch_comment_summaries



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

def submission_features(submission_batch, num_top_comments = 15, api_feat = api_feat):
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
        features['Author'] = features['Author'].name
        features['Subreddit'] = features['Subreddit'].display_name
        
        # Convert UTC timestamp to time of day (in minutes since beginning of UTC day)
        dtime_posted = datetime.datetime.fromtimestamp(features['Post time'])
        features['Post time'] = dtime_posted.hour*60 + dtime_posted.minute
        
        # Calculate age of the post (in minutes)
        features['Post age'] = (datetime.datetime.now() - dtime_posted).total_seconds()/60
        
        # Calculate upvotes per minute of age and comments per minute of age
        features['Upvote rate'] = features['Upvotes']/features['Post age']
        features['Comment rate'] = features['Comments']/features['Post age']
        
        
        # Call function to process comment data for single submissions
        (avg_up, std_up, gild, disting,
         op_comms, premiums) = get_toplevel_comment_info(subm, num_top_comments)
    
        # Assign to dictionary
        features['Avg top comments up rate'] = avg_up
        features['Std top comments up rate'] = std_up
        features['Num top comment gildings'] = gild
        features['Num top comment distinguished'] = disting
        features['Num op Comments'] = op_comms
        features['Num top comment premium auths'] = premiums

        # Append this submission's feature to the set of all features for the batch
        batch_features[subm] = features
    
    return batch_features




if __name__ == '__main__':
    
    t0 = datetime.datetime.now()
    
    subreddit_list = ['aww', 'history', 'askreddit']
    data = pull_and_process(reddit_auth = 1,
                            subreddit_list = subreddit_list,
                            num_posts = 100,
                            how = 'rising',
                            num_top_comments = 5
                           )
    df = pd.DataFrame(data).transpose()
    
    t1 = datetime.datetime.now()
    
    print('Total time was {:.2f} seconds to process all submissions.'.format((t1-t0).total_seconds()))