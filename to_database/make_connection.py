# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 13:32:20 2021

@author: Dillo
"""

import os

import boto3
import psycopg2

from get_db_auth import get_db_auth




def make_connection_if_cold():

    bucket_name = os.environ['bucket_name']
    db_auth_fname = os.environ['db_auth_fname']
    """
    Make connection if connection doesn't already exist. This takes advantage of 
    AWS Lambda's ability to leave connections open temporarily if the function
    is called regularly. No need to wait to establish connection if its still open.
 
    Parameters
    ----------
    bucket_name : str
        Name of s3 bucket where all files live.
    db_auth_fname : str
        Filename/AWS s3 object key for txt file containing keys for AWS Aurora
        Posgresql database access.
    
    Returns
    -------
    conn : psycopg2 connection object
        Contains database connection data and authorization.

    """
    
    try:
        if conn.closed == 0: # Zero if connection is still open
            pass
        else:
            # Nonzero if no connection established
            print('No connection, now establishing')
            conn = make_connection(bucket_name, db_auth_fname)
            print('Connection to dbase made')
    except NameError:
            # If no connection then 'conn' doesn't exist
            print('Error when searching for connection - now establishing')
            conn = make_connection(bucket_name, db_auth_fname)
            print('Connection to dbase made, moving to handler function')
    return conn




def make_connection(bucket_name, db_auth_fname):
    """
    Connect to AWS Aurora Postgresql database.

    Parameters
    ----------
    bucket_name : TYPE
        DESCRIPTION.
    db_auth_fname : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    
    """ 
    Set up call to AWS S3 bucket that contains the file for Reddit API 
    authorization.
    """
    s3 = boto3.resource('s3')
    
    # Load Reddit auth data
    db_auth_obj = s3.Object(bucket_name, db_auth_fname)
    db_auth_file = db_auth_obj.get()
    
    # Get auth object
    db_auth = get_db_auth(db_auth_file)

    # Make connection
    conn = psycopg2.connect(host     = db_auth.ENDPOINT,
                            port     = db_auth.PORT,
                            dbname   = db_auth.DBNAME,
                            user     = db_auth.USR,
                            password = db_auth.DBPWD,
                           )

    conn.autocommit = True
    
    return conn



