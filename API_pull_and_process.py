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

# Need to create a script that pulls and processes posts in batches. 

# Start by altering the processing scripts so that they can take batches of
# submissions instead of single submissions.

# First need to know what format the batches will come in. Lets start there.

reddit = praw.Reddit(client_id = 'h8BBe0NNslxi8g', 
                     client_secret = 'gd2EfD_bd9njZI9zngbiD1WhMJo8lA', 
                     user_agent = 'Chrome:AwardPredictor:v0.0.1 (by /u/drdnm)')


multi = reddit.subreddit('aww+history+askreddit')
multi_hot = multi.hot(limit = 225)



def pull_and_process(reddit_auth, subreddit_list, num_posts, how = 'rising'):
    
    reddit_subs_to_pull = subreddit_list_to_string(subreddit_list)
    subreddit_instance = reddit_subs_to_pull(limit = num_posts)
    
    all_submission_features = {}
    submission_batch = []
    for subm_num, subm in enumerate(subreddit_instance):
        
        submission_batch.append(subm)
        
        if ((subm_num + 1)%100 == 0) | (subm_num == num_posts - 1):
            batch_features = submission_features(submission_batch, num_top_comments = 5)
            submission_batch = []
            all_submission_features.update(batch_features)
      
    return all_submission_features

def subreddit_list_to_string(subreddit_list):
    subreddit_multi_name = ''
    for subreddit_name in subreddit_list:
        subreddit_multi_name += '+' + subreddit_name.strip()
    
    return subreddit_multi_name[1:]



def get_toplevel_comment_info(submission_batch, num_top_comments = 5):
    
    # 'num_top_level' controls the number of highest-upvoted top-level comments for which the replies will be counted
    
    t_del_1 = datetime.datetime.now()
    
    
    # Delete all "more comments" entries to avoid errors - puts a cap on max number of comments retrieved
    subm.comments.replace_more(limit = 0) 
    
    
    t_del_2 = datetime.datetime.now()
    t_del = (t_del_2 - t_del_1).total_seconds()
    
    

    # List of features of interest to pull from the API
    api_feat = {'Gilded': 'gilded',
                'Gildings': 'gildings',
                'Upvotes': 'ups',
                'Downvotes': 'downs',
                'Distinguished': 'distinguished',
                'Edited': 'edited',
                'Controversiality': 'controversiality',
                'OP comment': 'is_submitter'
               }
    
    
    
    t_feat_1 = datetime.datetime.now()
    
    
    
    # Iterate through all comments to extract their features
    max_number_of_comments = 3*num_top_comments
    comment_features = {}
    for comment_number, comment in enumerate(subm.comments):
        if comment_number > max_number_of_comments:
            break
            
        # For each comment build a dict to hold its features, indexed by comment id
        ID = comment.id
        comment_features[ID] = {}
        for feat_name in api_feat:
            comment_features[ID][feat_name] = comment.__dict__[api_feat[feat_name]]
               
        # Calculate age of comment (in minutes)
        comment_dtime = datetime.datetime.fromtimestamp(comment.created_utc)
        now_dtime = datetime.datetime.now()
        comment_features[ID]['Age'] = (now_dtime - comment_dtime).total_seconds()/60
        
        # Calculate upvote rate (per minute)
        comment_features[ID]['Upvote rate'] = comment_features[ID]['Upvotes']/comment_features[ID]['Age']

    
    
    t_feat_2 = datetime.datetime.now()
    t_feat = (t_feat_2 - t_feat_1).total_seconds()
    
    t_reply_1 = datetime.datetime.now()

    
    
    # Calculate average number of 2nd-level replies for top comments:
    #    (number of comments controlled by 'num_top_level' variable)
    
    #     First need to order the top-level comments by upvotes in order to grab the top ones
    ups = [(ID, comment_features[ID]['Upvotes']) for ID in comment_features]
    ups_by_comme = (pd.DataFrame(ups)
                      .rename(columns = {0:'Comment ID', 1: 'Comment Ups'})
                      .sort_values(by = 'Comment Ups', ascending = False)
                      .iloc[:num_top_comments,:]
                      .reset_index(drop = True)
                   )
    #     For each of the top-n comments now grab all replies and count them up 
    num_replies = [(comment.id, len(comment.replies.__dict__['_comments'])) 
                   for comment in subm.comments.__dict__['_comments'] 
                   if comment.id in ups_by_comme['Comment ID'].to_list()
                  ]
    num_replies = (pd.DataFrame(num_replies)
                     .rename(columns = {0:'Comment ID', 1:'Num Replies'})    
                  )
    #     Merge these on the 'Comment ID' column
    top_comment_performance = ups_by_comme.merge(num_replies, on = 'Comment ID')
    
    #     Calculate the number of upvotes per minute and replies per minute since the comment was created
    top_comment_performance['Upvote rate'] = [top_comment_performance.loc[j, 'Comment Ups'] /
                                              comment_features[top_comment_performance.loc[j, 'Comment ID']]['Age']
                                              for j in top_comment_performance.index
                                             ]
    top_comment_performance['Reply rate'] = [top_comment_performance.loc[j, 'Num Replies'] /
                                              comment_features[top_comment_performance.loc[j, 'Comment ID']]['Age']
                                              for j in top_comment_performance.index
                                             ]
    #     Calculate average and standard deviation of the rates, append them to the feature dictionary
    Avg_up_rate = top_comment_performance['Upvote rate'].mean()
    Std_up_rate = top_comment_performance['Upvote rate'].std()
    Avg_reply_rate = top_comment_performance['Reply rate'].mean()
    Std_reply_rate = top_comment_performance['Reply rate'].std()
    
    
    
    t_reply_2 = datetime.datetime.now()
    t_reply = (t_reply_2 - t_reply_1).total_seconds()
    
    
    # There is an opportunity to create more features out of the comments. These could include:
    #    explore the success of comments/replies made by the submitter of the original post
    #    look at the distribution of comment upvotes (or of comment replies)
    #    look at the average controversiality among comments or replies to comments
    #    calculate the rate of gildings among comments and/or comment replies
    
    return Avg_up_rate, Std_up_rate, Avg_reply_rate, Std_reply_rate, t_feat, t_reply, t_del





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

def submission_features(submission_batch, num_top_comments = 5, api_feat = api_feat):
    
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
    
    

# =============================================================================
#   Extract and process comments
# =============================================================================

# =============================================================================
#   Pulling comments takes approx 1-2 seconds per submission, too slow.
#   Need to find a way to batch-pull top-n comments simultanesously from all 
#   100 submissions in the submission_batch. Until then I will not process
#   any comments.
#   
#   The PSAW api-wrapper for pushift.io (https://github.com/dmarx/psaw) might
#   be able to help since it can "use pushshift search to fetch ids and then 
#   use praw to fetch objects." This will be further explored as an option
#   for speeding up the process.
# =============================================================================

# =============================================================================
#     if subm.num_comments == 0:
#         Avg_up_rate, Std_up_rate, Avg_reply_rate, Std_reply_rate = (0, 0, 0, 0)
#     else:
#         Avg_up_rate, Std_up_rate, Avg_reply_rate, Std_reply_rate, t_feat, t_reply, t_del = get_toplevel_comment_info(subm, num_top_comments)
#     
#     features['Avg top comments up rate'] = Avg_up_rate
#     features['Std top comments up rate'] = Std_up_rate
#     features['Avg top comments reply rate'] = Avg_reply_rate
#     features['Std top comments reply rate'] = Std_reply_rate
# =============================================================================

    
    return features