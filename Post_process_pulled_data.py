# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 10:55:49 2020

@author: Dillon
"""

import os
import pandas as pd
import datetime
from API_pull_and_process import create_auth_dict
import praw

def combine_data(data_path = './data'):
    # Path where pulled and processed data is stored
    #data_path = './data/'
    # Path where the combined dataframe from all pulled and processed data is to be stored
    combined_dframe_path = os.path.join(data_path,
                                        'combined/'
                                       )
    # Build filename that denotes current time and date
    t1 = datetime.datetime.utcnow()
    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]
    # Filename for combined dataframe of all pulled and processed data
    combined_filename = (month + '-' + day + '_at_' + hour + minute + 'utc_ALL.pkl')
    
    # Raise exception of the data-storage directory does not exist
    if not os.path.isdir(data_path):
        raise Exception('There is no data to process.')
    
    # Iterate over all pickled data files in the data directory, append them to list
    dataframes = []
    for fname in os.listdir(data_path):
        if fname.endswith('.pkl'):
            dframe = pd.read_pickle(os.path.join(data_path,fname))
            dataframes.append(dframe)
    
    # Raise exception if there were no pickle files in the directory
    if len(dataframes) == 0:
        raise Exception('There is no data to process.')
    
    # Create directory for combined dataframe if need be
    if not os.path.isdir(combined_dframe_path):
        os.mkdir(combined_dframe_path)

    # Combine all dataframes in list, remove dubplicates
    df = pd.concat(dataframes)
    df = df[~df.index.duplicated(keep='first')]
    

    # Remove entries which are already gilded
    num_gilds = df['Gildings'].apply(lambda x: count_gildings(x))
    gildings_bool_mask = num_gilds == 0
    df = df[gildings_bool_mask]
    
    # Store combined file to drive
    df.to_pickle(os.path.join(combined_dframe_path, combined_filename))
    
    return print('All current data merged at {} UTC.'.format(hour + minute))

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



def pull_gildings(reddit_auth_file, which = 'newest', 
                  combined_dframe_path = './data/combined/'
                 ):
    
    auth_dict = create_auth_dict(reddit_auth_file)
    reddit = praw.Reddit(client_id = auth_dict['client_id'],
                         client_secret = auth_dict['client_secret'],
                         user_agent = auth_dict['user_agent'],
                         )

    
    if which == 'newest':
        modification_times = {}
        for fname in os.listdir(combined_dframe_path):
            if fname.endswith('_ALL.pkl'):
                fpath = os.path.join(combined_dframe_path, fname)
                modification_times[fname] = os.path.getmtime(fpath) 
        
        filename_to_fetch = max(modification_times,
                                key = modification_times.get
                               )
    else:
        filename_to_fetch = which
        
    filepath_to_fetch = os.path.join(combined_dframe_path, filename_to_fetch)
    df = pd.read_pickle(filepath_to_fetch)
    submission_ids= df['ID']
    
    fullname_list = ['t3_' + ID for ID in submission_ids]

    submissions = reddit.info(fullname_list)
    
    gilding_data = {}
    for subm in submissions:
        gilds = subm.gildings
        try:
            silvers = gilds['gid_1']
        except KeyError:
            silvers = 0
        try:
            golds = gilds['gid_2']
        except KeyError:
            golds = 0
        try:
            platinums = gilds['gid_3']
        except KeyError:
            platinums = 0
        
        gilding_data[subm.id] = {}
        gilding_data[subm.id]['Silver'] = silvers
        gilding_data[subm.id]['Gold'] = golds
        gilding_data[subm.id]['Platinum'] = platinums

# Sample output dict
# =============================================================================
#     gilding_data = {'adad': {'silver':2, 'gold': 1, 'platinum': 3},
#                     'ags9': {'silver':11, 'gold': 7, 'platinum': 1},
#                     '10cad': {'silver':22, 'gold': 99, 'platinum': 66},
#                     }
# =============================================================================
        
    gildings = pd.DataFrame(gilding_data).transpose()
    
    
    t1 = datetime.datetime.utcnow()
    month, day, hour, minute  = [ '0' + str(getattr(t1, k)) 
                                 if len(str(getattr(t1, k))) == 1
                                 else str(getattr(t1, k))
                                 for k in ['month', 'day', 'hour', 'minute']
                                ]

    gild_fname = (filename_to_fetch.split('.')[0] +
                  '_gildings_fetched_on_' +
                  month + '-' + day + '_at_' +
                  hour + minute + 'utc.pkl'
                 )

    gild_fpath = os.path.join(combined_dframe_path, gild_fname)
    gildings.to_pickle(gild_fpath)
    
    return print('Gildings fetched at {}.'.format(hour+minute+'UTC'))