# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 10:33:40 2021

@author: Dillo
"""

import json

import boto3


def get_fnames(event, context):
    
    bucket_name = event['bucket_name']  
    staging_dir = event['staging_dir']
    completed_dir = event['completed_dir']
    error_dir = event['error_dir']
    
    schema_name = event['schema_name']
    table_name = event['table_name']
    
    """
    Retrieves list of file keys within an AWS s3 bucket, these files will
    eventually be loaded, parsed, inserted into AWS Auorora Postgresql table,
    then moved to a new directory.

    Parameters
    ----------
    bucket_name : str
        Name of s3 bucket where all files live.
    staging_dir : str
        Name of directory inside bucket where files are currently located.
    completed_dir : str
        Name of directory inside bucket where files are to be moved if they are
        correctly parsed and loaded in to SQL table.
    error_dir : str
        Name of directory inside bucket where files are to be moved if there is
        an error in the process of parsing and loading.
    schema_name : str
        Name of the SQL table schema for data to be loaded in to.
    table_name : str
        Name of the SQL table name for data to be loaded in to.

    Returns
    -------
    dict
        Returns all the above info for next function in the procedure, with the
        addition of the list of file keys located within staging_dir.

    """    
    
    
    files_iterator = bucket_contents_iterator(bucket_name, staging_dir)
    
    # Iterate through files, append their keys to a list
    fnames_list = []
    for metadata in files_iterator:
        fname = metadata['Key']
        fnames_list.append(fname)
    
    # Occasionally will pick up the directory name itself inside list, not sure
    #   why, but if its there it must be deleted from list
    if staging_dir + '/' in fnames_list:
        fnames_list.remove(staging_dir + '/')
    
    
    return_dict = {'bucket_name': bucket_name,
                   'staging_dir': staging_dir,
                   'completed_dir': completed_dir,
                   'error_dir': error_dir,
                   'schema_name': schema_name,
                   'table_name': table_name,
                   'fnames_list': fnames_list
                   }
    
    return {
            'statusCode': 200,
            'body': json.dumps(return_dict)
            }
    
    
    
    
def bucket_contents_iterator(bucket_name, staging_dir):
    """
    Generator that iterates over all objects in a given s3 bucket (each file
    in bucket yielding a dictionary of metadata)

    Parameters
    ----------
    bucket_name : str
        name of s3 bucket.

    Yields
    ------
    item : dict
        metadata for an object.

    """
    
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket = bucket_name,
                                       Prefix = staging_dir,
                                      )

    for page in page_iterator:
        if page['KeyCount'] > 0:
            for item in page['Contents']:
                yield item  