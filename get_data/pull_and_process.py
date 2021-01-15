# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 11:58:16 2020

@author: Dillo
"""

from datetime import datetime as dt
import requests
import json
import os
import time
import exceptions

import praw
import pandas as pd




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
    auth : pull_and_process.auth
        Authorization object with properites needed to access the Reddit API.
    """

    def __init__(self, reddit_auth_file):
        f = open(reddit_auth_file, "r")
        for var, line in enumerate(f):
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
        f.close()             




def pull_and_process(reddit_auth_file, subreddit_list, num_posts, 
                     num_top_comments = 15, how = 'new'
                    ):
    """
    Makes API call (in batches of 100 submissions) and passes each batch
    of submissions on to have features extracted.

    Parameters
    ----------
    reddit_auth_file : str
        Filename of .txt file containing Reddit API keys.
    subreddit_list : list
        List of strings indicating the subreddits to be included in the 
        analysis.
    num_posts : int
        The total number of submissions to be retrieved.
    num_top_comments : int, optional
        How many of the submission's comments (sorted by comments' upvotes,
        descending) should be used in the analysis. Younger submissions may not
        yet have many comments. The default is 15.
    how : str, optional
        How the submissions are to be sorted within Reddit. The default is 
        'rising'.

    Returns
    -------
    all_submission_features : TYPE
        DESCRIPTION.

    """
    
    auth = get_auth(reddit_auth_file)
    
    reddit = praw.Reddit(client_id = auth.client_id,
                         client_secret = auth.client_secret,
                         user_agent = auth.user_agent,
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
            batch_features = submission_features(auth,
                                                 submission_batch,
                                                 num_top_comments,
                                                )
            submission_batch = []
            all_submission_features.update(batch_features)
      
    return all_submission_features




def subreddit_list_to_string(subreddit_list):
    """
    Converts a list of subreddits to a single string for passing to Reddit API.

    Parameters
    ----------
    subreddit_list : list
        A list of all subreddits to be included in the analysis.

    Returns
    -------
    subreddit_multi_name: str
        A string containing all subreddits to be analyzed, for use in Reddit
        API.

    """
    
    subreddit_multi_name = ''
    for subreddit_name in subreddit_list:
        subreddit_multi_name += '+' + subreddit_name.strip()
    
    return subreddit_multi_name[1:]




def get_toplevel_comment_info(auth, submission, num_top_comments = 15):
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
    submission_id = str(submission.id)
    submission_sub = str(submission.subreddit)
    url = base_url + submission_sub + '/comments/' + submission_id + '.json'

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




def submission_features(auth, submission_batch, 
                        num_top_comments = 15, api_feat = api_feat
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




def get_subreddit_list(subreddits_filepath):
    """
    Reads and parses txt file containing all subreddits to include in analysis.

    Parameters
    ----------
    subreddits_filepath : str
        Filepath of txt file containing list of subreddits, delimited by ',/n'.

    Returns
    -------
    subreddit_list: list
        List of all subreddits.

    """
    with open(subreddits_filepath, "r") as f:
        subreddit_list = f.read().split(',\n')
        
    return subreddit_list




def get_reddit_submissions(sortedby = ['new'],
                           num_posts = 500,
                           num_top_comments = 15,
                           subreddits_fpath = './subreddits.txt',
                           reddit_auth_file = '../auth.txt',
                           savepath = './data/'
                          ):
    """
    Wrapper which structures the pull_and_process procedure. Iterates over all
    sortedby types, pulls and processes data, adds to Pandas dataframe, then
    saves as a pickle file.
    
    Each iteration within sortedby can be time consuming and the API may
    timeout during. The number of retry attempts per sortedby value is set
    to 3 with a 60 second delay between attempts.

    Parameters
    ----------
    sortedby : list, optional
        List of methods by which the posts will sorted within Reddit. Sorted 
        by types include "new", "hot", "rising". The default is ['new'].
    num_posts : int, optional
        Number of posts to pull. Provides and upper bound, may end up with 
        fewer, unsure of why but I blame the API. The default is 500.
    num_top_comments : int, optional
        How many of the comments (sorted by number of upvotes, descending) will
        be pulled for analysis. The default is 15.
    subreddit_list : list, optional
        List containing strings defining subreddits to include within the 
        model. A default list is currently hard coded The default is 
        subreddit_list.
    reddit_auth_file : str, optional
        Filepath of .txt file containg Reddit keys. The default is 'auth.txt'.
    savepath : str, optional
        Defines directory where pickle file will be saved. The default is 
        './data/'.

    Returns
    -------
    None. A file with the scraped data will be saved to disk in the designated
    directory with a unique filename specifying the parameters used. E.g.
    "./data/01-15_at_1530utc_sortedby_new_using_10_comments.pkl"

    """
    
    if not os.path.isdir(savepath):
        os.mkdir(savepath)
    
    subreddit_list = get_subreddit_list(subreddits_fpath)
    
    max_tries = 3
    secs_betw_attempts = 60
    secs_betw_sortedby_types = 30
    num_sortedby_types = len(sortedby)
    
    for j, how in enumerate(sortedby):
        num_tries = 0
        data_obtained = False
        while not data_obtained:
            num_tries += 1
            if num_tries > max_tries:
                raise exceptions.ApiRetryAttemptsExceeded("""API timed out 
                                                          after {} tries"""
                                                          .format(max_tries)
                                                          )
            try:
                data = pull_and_process(reddit_auth_file = reddit_auth_file,
                                        subreddit_list = subreddit_list,
                                        num_posts = num_posts,
                                        how = how,
                                        num_top_comments = num_top_comments
                                       )
                df = pd.DataFrame(data).transpose()
            
                fname = build_filename(sort_and_comments = True,
                                       sortedby = how,
                                       num_top_comments = num_top_comments,
                                       )
                file_path = os.path.join(savepath, fname)
                df.to_pickle(file_path)

                data_obtained = True
            except:
                time.sleep(secs_betw_attempts)
        if not (j ==  num_sortedby_types - 1):
            time.sleep(secs_betw_sortedby_types)
                
    return None




def build_filename(sort_and_comments = False, sortedby = None, num_top_comments = None):
    """
    Constructs a unique filename based on parameters chosen to pull from API.

    Parameters
    ----------
    sort_and_comments : bool, optional
        Whether to include information about how the posts were sorted ('new',
        'rising', etc.) and the number of top-level comments used in the
        analysis. The default is False.
    sortedby : str, optional
        How the posts were sorted within Reddit ('new', 'hot', etc.). The 
        default is None.
    num_top_comments : int or None, optional
        The number of top-level comments used in the analysis. The default is 
        None.

    Returns
    -------
    filename : TYPE
        DESCRIPTION.

    """
    if sort_and_comments:
        if (sortedby == None) | (num_top_comments == None):
            raise exceptions.InsufficientDataPassed("""Must include sortedby 
                                                    and num_toplevel_comments
                                                    values if choosing to use
                                                    sort_and_comments info
                                                    for the filename""")
    
    t1 = dt.utcnow()

    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]
    filename_dtime = month + '-' + day + '_at_' + hour + minute + 'utc'

    
    if sort_and_comments:
        filename = ( filename_dtime + 
                    '_sortedby_' + sortedby + 
                    '_using_' + str(num_top_comments) + '_comments' +
                    '.pkl'
                    )
    else:
        filename = filename_dtime + '.pkl'
    
    
    return filename
