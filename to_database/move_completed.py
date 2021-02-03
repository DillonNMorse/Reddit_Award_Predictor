# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 15:55:19 2021

@author: Dillo
"""

import json

import boto3



def move_to_completed(event, context):
    inputs_dict = json.loads(event['body'])

    num_names_left = event['num_names_left']
    
    bucket_name = inputs_dict['bucket_name']
    staging_dir = inputs_dict['staging_dir']
    completed_dir = inputs_dict['completed_dir']
    error_dir = inputs_dict['error_dir']
    
    schema_name = inputs_dict['schema_name']
    table_name = inputs_dict['table_name']
    fnames_list = inputs_dict['fnames_list']
    fname_processed = inputs_dict['fname_processed']
    
    """
    Moves files from AWS s3 staging directory to error directory if there was
    an error during processing or writing of fname_processed data to SQL table.

    Parameters
    ----------
    num_names_left : int
        The number of files in the list which still need to be processed.
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
    fnames_list : list
        List of strings, the keys of all files within staging directory. Note 
        that the key strings also contain directory information.
    fnames_processed : str
        The filename/AWS s3 object key of the file which was just processed and
        now needs to be moved

    Returns
    -------
    dict
        Passes through most input values. The dictionary explicitly passes the 
        number of files left in the list which still need to be processed (for
        use by AWS step function to determine whether to continue iteration).

    """      
    
    
    s3_resource = boto3.resource('s3')
    
    # Split directory name from full key, only keep fname
    fname_proc_wo_dir = fname_processed.split('/')[-1]

    # Append bucket name to create full AWS s3 filepath of file to be copied
    old_key = '/'.join([bucket_name, fname_processed])
        
    # Adderror directory to filename to indicate location where file copied to
    new_key = '/'.join([completed_dir, fname_proc_wo_dir])
    
    # Copy 
    (s3_resource.Object(bucket_name, new_key)
                .copy_from(CopySource = old_key)
                )
    
    # Delete the old file from staging directory
    s3_resource.Object(bucket_name, fname_processed).delete()
    
    
    return_dict = {'bucket_name': bucket_name,
                   'staging_dir': staging_dir,
                   'completed_dir': completed_dir,
                   'error_dir': error_dir,
                   'schema_name': schema_name,
                   'table_name': table_name,
                   'fnames_list': fnames_list,
                   }
    
    return {
            'statusCode': 200,
            'num_names_left': num_names_left,
            'body': json.dumps(return_dict)
            }