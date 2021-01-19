# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 11:56:03 2021

@author: Dillo
"""

import pickle
import os


def combine_reddit_data(posts_fpath, comments_fpath, directory):
    

    posts = pickle.load(open(posts_fpath,'rb'))
    comments = pickle.load(open(comments_fpath,'rb'))
    
    # Combine the two dicts, removing any posts already gilded when pulled.
    for ID in posts:
        num_gilds = count_gildings(posts[ID]['Gildings'])
        if num_gilds > 0:
            del posts[ID]
        else:
            posts[ID].update(comments[ID])
        
    complete_dtime = posts_fpath[27:]
    complete_fpath = os.path.join(directory, 'complete_data_' + complete_dtime)
    pickle.dump(posts, open(complete_fpath, 'wb'))   
        
        
    return




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