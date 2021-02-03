# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 12:32:45 2021

@author: Dillo
"""

import re




def transform_all_data_dicts_for_sql_schema(fname, data):
    """
    Iterate through a python dictionary to transform data to fit a specific 
    SQL table schema. The keys correspond to Reddit post ID's, each value in 
    the dict is itself a dictionary containing all the Reddit post data. The 
    keys of the internal dicts are the features to be extracted, corresponding
    to SQL table columns.

    Parameters
    ----------
    fname : str
        The filename/AWS s3 key to be iterated through. The name contains some
        useful metadata about when/how the posts were scraped.
    data : dict
        Dictionary of dictionaries, each internal dict contains the feature
        data to be transformed and loaded.

    Returns
    -------
    new_data_dicts : dict
        Dictionary of dictionaries, each internal dict contains the transformed
        data, ready to be loaded in to SQL table.

    """
    
    new_data_dicts = {}
    for key in data.keys():
        new_data_dicts[key] = transform_dict_dtypes_for_sql_schema(fname,
                                                                   data[key],
                                                                  )
    return new_data_dicts




def transform_dict_dtypes_for_sql_schema(fname, data):
    """
    Applies data transformation to a python dictionary containing data for an
    individual Reddit post.

    Parameters
    ----------
    data : dict
        Python dictionary containing data for an individual Reddit post.
    fname : str
        Filename/AWS s3 object key, contains useful metadata about when/how
        data was scraped.

    Returns
    -------
    new_dict : dict
        Python dictionary containing data for an individual Reddit post,
        transformed to fit the schema for our SQL table.

    """
    
    # Make copy of dict to alter, will be returned as new, transformed dict
    # Combine two-word keys in to single-words for SQL
    new_dict = {}
    for key, value in data.items():
        new_key = combine_words(key)
        new_dict[new_key] = value
    
    # Add how sorted - purely for my info, not useful as a predictive feature
    how = re.search(r'sortedby_([\w\s]+)_', fname).group(1)
    new_dict['How_sorted'] = how
    
    # Add scrape key to post ID to ensure unique entries for SQL PRIMARY KEY
    new_dict['ID'] = new_unique_ID_with_scrape_time(fname, data['ID'])
    
    # Unpack gildings dict (comes from first scrape)
    silvers, golds, platinums = count_gildings(data['Gildings'])
    new_dict['Initial_silver'] = silvers
    new_dict['Initial_gold'] = golds
    new_dict['Initial_platinum'] = platinums
    del new_dict['Gildings']
    
    # Deal with all possible "None" types
    new_dict['Views'] = convert_NONE_to_0(data['Views'])
    new_dict['Likes'] = convert_NONE_to_0(data['Likes'])
    
    # Convert distinguished to boolean
    new_dict['Distinguished'] = convert_NONE_to_bool(data['Distinguished'])
    
    # Convert any None's in 'Discussion type' to a string
    new_dict['Discussion_type'] = convert_discussion_to_str(data['Discussion type'])
    
    # Unpack content categories, taking first element of list
    new_dict['Content_categories'] = unpack_content_cats(data['Content categories'])
    
    # Convert some feats to floating type
    new_dict['distinguished_rate'] = float(data['distinguished_rate'])
    new_dict['op_comment_rate'] = float(data['op_comment_rate'])
    new_dict['premium_auth_rate'] = float(data['premium_auth_rate'])
    new_dict['gild_rate'] = float(data['gild_rate'])
    new_dict['std_up_rate'] = float(data['std_up_rate'])
    new_dict['avg_up_rate'] = float(data['avg_up_rate'])
    
    # Convert Edited to boolean type
    new_dict['Edited'] = convert_edited_to_bool(data['Edited'])
    
# =============================================================================
#     # Insert nulls when old dict missing keys (specifically for final upvotes
#     #   and final comments which got added after some data already collected)
#     if not 'Final_num_comments' in new_dict.keys():
#         new_dict['Final_num_comments'] = 'NULL'
#     if not 'Final_upvotes' in new_dict.keys():
#         new_dict['Final_upvotes'] = 'NULL'        
# =============================================================================
        
    
    
    return new_dict




def combine_words(string):
    """
    Combines multi-word strings in to a single word with underscores.

    Parameters
    ----------
    string : str
        The string to be combined.

    Returns
    -------
    new_string : str
        A string with no whitespace.

    """
    
    words = string.split()
    new_string = '_'.join(words)
    
    return new_string




def convert_discussion_to_str(x):
    """
    Changes python NONE-type to the string "None".

    Parameters
    ----------
    x : string or NONE-type
        The value to be changed if necessary.

    Returns
    -------
    x : string
        Either the string "None" or pass through the original value x.

    """
    
    if type(x) == type(None):
        return 'None'
    else:
        return x




def unpack_content_cats(x):
    """
    Unpack list of category types, or return string "None" if there are no 
    category types (only some subreddits use this feature).

    Parameters
    ----------
    x : list or NONE-type
        The category of the Reddit post.

    Returns
    -------
    category : str
        The category of the Reddit post.

    """
    
    if type(x) == type(None):
        return 'None'
    else:
        # Take first element, all observed posts have *at most* one entry
        return x[0]




def convert_NONE_to_bool(x):
    """
    Create a categorical variable, "None" or "not-none", given an input

    Parameters
    ----------
    x : str or NONE-type
        The input value to be checked.

    Returns
    -------
    variable : bool
        Either False or True, was the input NONE-type or not.

    """
    
    if type(x) == type(None):
        return bool(0)
    else:
        return bool(1)




def convert_NONE_to_0(x):
    """
    Either returns 0 (corresponding to NONE-type input), the integer value of 
    x if input is numerical, or -1 if input is other.

    Parameters
    ----------
    x : (any)
        Input may be anything.

    Returns
    -------
    value : int
        Either -1, 0, or a positive integer.

    """
    
    if type(x) == type(None):
        return 0
    else:
        try:
            return int(x)
        except ValueError:
            return -1




def count_gildings(gild_dict):
    """
    Given a Reddit dictionary of gildings, unpack and extract the number of 
    each medal type.

    Parameters
    ----------
    gild_dict : dict
        Reddit dictionary of gildings.

    Returns
    -------
    silvers : int
        Number of silvers awarded to the Reddit post.
    golds : int
        Number of golds awarded to the Reddit post.
    platinums : int
        Number of platinums awarded to the Reddit post.

    """
    
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
        
    return silvers, golds, platinums




def new_unique_ID_with_scrape_time(fname, old_ID):
    """
    Combines Reddit post ID with scraping metadata to make new unique IDs as 
    PRIMARY KEYS within SQL table. The Reddit ID's are unique to Reddit, but a 
    particular Reddit post might be pulled multiple times over the course of a
    day.

    Parameters
    ----------
    fname : str
        The filename/AWS s3 object key, contains metadata about scraping (both
        when scraped as well as how Reddit was sorted during scraping).
    old_ID : str
        Unique ID assocated to each Reddit post, 6 alphanumeric characters.

    Returns
    -------
    new_ID : str
        Unique ID for each post scraped - contains Reddit post ID as well as 
        scraping metadata.

    """
    
    scrape_month = fname[-20:-18]
    scrape_day = fname[-17:-15]
    scrape_utc = fname[-11:-7]
    how = re.search(r'sortedby_([\w\s]+)_', fname).group(1)
    
    scrape_key = how + scrape_month + scrape_day + scrape_utc
    new_ID = old_ID + '_' + scrape_key
    
    return new_ID




def convert_edited_to_bool(x):
    """
    Converts Reddit edited feature in to a boolean. False is left alone,
    whereas the UTC epoch for an edit is instead converted to True.

    Parameters
    ----------
    x : bool or float
        Either False (if no edits made) or the UTC epoch when edit was made. 

    Returns
    -------
    x : bool
        Was the post edited.

    """
    
    if x != False:
        x = True
        
    return x