# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 11:58:16 2020

@author: Dillo
"""


import praw
import pprint
from datetime import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import requests
import numpy as np
import json
import os

# Need to create a script that pulls and processes posts in batches. 

# Start by altering the processing scripts so that they can take batches of
# submissions instead of single submissions.

# First need to know what format the batches will come in. Lets start there.

# =============================================================================
# client_id = 'h8BBe0NNslxi8g'
# client_secret = 'gd2EfD_bd9njZI9zngbiD1WhMJo8lA'
# user_agent = 'Chrome:AwardPredictor:v0.0.1 (by /u/drdnm)'
# =============================================================================




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


def create_auth_dict(reddit_auth_file):
    
    auth = {}
    f = open(reddit_auth_file, "r")
    for var, line in enumerate(f):
        if var >= 3:
            break
        variable_name = line.split('=')[0].strip()
        variable_value = line.split('=')[1].strip()
        auth[variable_name] = variable_value
    f.close()     
    
    return auth


def pull_and_process(reddit_auth_file, subreddit_list, num_posts, 
                     num_top_comments = 15, how = 'rising'
                    ):
    
    auth_dict = create_auth_dict(reddit_auth_file)
    
    reddit = praw.Reddit(client_id = auth_dict['client_id'],
                         client_secret = auth_dict['client_secret'],
                         user_agent = auth_dict['user_agent'],
                         )
    
    reddit_subs_to_pull = subreddit_list_to_string(subreddit_list)
    subreddit_instance = getattr(reddit.subreddit(reddit_subs_to_pull),
                                 how
                                )(limit = num_posts)
                         
    
    all_submission_features = {}
    submission_batch = []
    for subm_num, subm in enumerate(subreddit_instance):
        
        submission_batch.append(subm)
        
        if ((subm_num + 1)%100 == 0) | (subm_num == num_posts - 1):
            batch_features = submission_features(auth_dict,
                                                 submission_batch,
                                                 num_top_comments,
                                                )
            submission_batch = []
            all_submission_features.update(batch_features)
      
    return all_submission_features






def subreddit_list_to_string(subreddit_list):
    subreddit_multi_name = ''
    for subreddit_name in subreddit_list:
        subreddit_multi_name += '+' + subreddit_name.strip()
    
    return subreddit_multi_name[1:]




def get_toplevel_comment_info(auth_dict, submission, num_top_comments = 15):
    
    # Given a single submission, process its top 'num_top_comments' comments
    
    # Set up authentication for Reddit API
    #   Due to some rate-limiting, cannot practically use PRAW for comments
    base_url = str('https://www.reddit.com/r/')
    auth = requests.auth.HTTPBasicAuth(auth_dict['client_id'],
                                       auth_dict['client_secret'],
                                       )
    headers = {'user-agent': auth_dict['user_agent']}
    
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
    current_tstmp = dt.utcnow()
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





# =============================================================================
# def process_comments_for_batch(submission_batch, num_top_comments = 15):
#     # Set up dictionary to hold data, indexed by individual Reddit submissions
#     batch_comment_summaries = {}
#     
#     # Iterate through all submissions in the batch
#     for submission in submission_batch:
#         # Create a dictionary to hold data for each individual summary
#         batch_comment_summaries[submission] = {}
#         
#         # Call function to process comment data for single submissions
#         (Avg_up_rate, Std_up_rate, gild_rate, distinguished_rate,
#             op_comment_rate, premium_auth_rate,
#         ) = get_toplevel_comment_info(submission, num_top_comments)
#     
#         # Assign to dictionary
#         batch_comment_summaries[submission]['Avg_up_rate'] = Avg_up_rate
#         batch_comment_summaries[submission]['Std_up_rate'] = Std_up_rate
#         batch_comment_summaries[submission]['Gild_rate'] = gild_rate
#         batch_comment_summaries[submission]['Distinguished_rate'] = distinguished_rate
#         batch_comment_summaries[submission]['Op_comment_rate'] = op_comment_rate
#         batch_comment_summaries[submission]['Premium_auth_rate'] = premium_auth_rate
#         
#         
#     return batch_comment_summaries
# =============================================================================



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

def submission_features(auth_dict, submission_batch, 
                        num_top_comments = 15, api_feat = api_feat
                       ):
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
        ) = get_toplevel_comment_info(auth_dict, subm, num_top_comments)
    
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





    

    
subreddit_list = ['aww',
                  'history',
                  'askreddit',
                  'funny',
                  'announcements',
                  'pics',
                  'todayilearned',
                  'science',
                  'iama',
                  'blog',
                  'videos',
                  'worldnews',
                  'gaming',
                  'movies',
                  'music',
                  'news',
                  'gifs',
                  'askscience',
                  'explainlikeimfive',
                  'earthporn',
                  'books',
                  'television',
                  'lifeprotips',
                  'sports',
                  'diy',
                  'showerthoughts',
                  'space',
                  'jokes',
                  'tifu',
                  'food',
                  'photoshopbattles',
                  'art',
                  'internetisbeautiful',
                  'mildlyinteresting',
                  'getmotivated',
                  'history',
                  'nottheonion',
                  'gadgets',
                  'dataisbeautiful',
                  'futurology',
                  'documentaries',
                  'listentothis',
                  'personalfinance'
                  'philosophy',
                  'nosleep',
                  'creepy',
                  'oldschoolcool',
                  'upliftingnews',
                  'writingprompts',
                  'twoxchromosone',
                  'fermentation',
                  'spicy',
                  'fitness',
                  'technology',
                  'bestof',
                  'adviceanimals',
                  'politics',
                  'atheism',
                  'programming',
                  'entertainment',
                 ]
    
def get_reddit_submissions(sortedby = 'new', num_posts = 500, num_top_comments = 10,
                           subreddit_list = subreddit_list, reddit_auth_file = 'auth.txt',
                           savepath = './data/'
                          ):
    if not os.path.isdir(savepath):
        os.mkdir(savepath)
    
    for how in sortedby:
        t0 = dt.utcnow()   
    
        data = pull_and_process(reddit_auth_file = reddit_auth_file,
                                subreddit_list = subreddit_list,
                                num_posts = num_posts,
                                how = how,
                                num_top_comments = num_top_comments
                               )
        df = pd.DataFrame(data).transpose()
        
        t1 = dt.utcnow()

        fname = build_filename(sort_and_comments = True,
                               sortedby = how,
                               num_top_comments = num_top_comments,
                               )
        file_path = os.path.join(savepath, fname)
        df.to_pickle(file_path)
        
        print('Total time was {:.2f} seconds to process all submissions.'
              .format((t1-t0).total_seconds())
             )
        print('Pulled {} submissions sorted by {}. \n'.format(df.shape[0], how))
    
    return None


def build_filename(sort_and_comments = False, sortedby = None, num_top_comments = None):
    t1 = dt.utcnow()

    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]
    filename_dtime = month + '-' + day + '_at_' + hour + minute + 'utc'
    
# =============================================================================
#     print('filename_dtime: ', filename_dtime, 'Type: ', type(filename_dtime), '\n')
#     print('sortedby: ', sortedby, 'Type: ', type(sortedby), '\n')
#     print('str(num_top_comments): ', str(num_top_comments), 'Type: ', type(str(num_top_comments)), '\n')
# =============================================================================
    
    if sort_and_comments:
        filename = ( filename_dtime + 
                    '_sortedby_' + sortedby + 
                    '_using_' + str(num_top_comments) + '_comments' +
                    '.pkl'
                    )
    else:
        filename = filename_dtime + '.pkl'
    
    
    return filename