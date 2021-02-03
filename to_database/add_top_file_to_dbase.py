# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 10:42:34 2021

@author: Dillo
"""

import json
import pickle as pkl

import boto3
import psycopg2
import psycopg2.extras

from make_connection import make_connection_if_cold
from transform_data_dicts_for_sql import transform_all_data_dicts_for_sql_schema
from schema import create_table_string
        



"""
Will only re-open connection to SQL table if one isn't already established,
this takes advantage of the fact that AWS Lambda functions don't 
necessarily terminate connections immediately after Lambda function completion
"""
conn = make_connection_if_cold()




def add_top_file_to_dbase(event, context):
    inputs_dict = json.loads(event['body'])    
    
    bucket_name = inputs_dict['bucket_name']
    staging_dir = inputs_dict['staging_dir']
    completed_dir = inputs_dict['completed_dir']
    error_dir = inputs_dict['error_dir']
    
    fnames_list = inputs_dict['fnames_list']
    schema_name = inputs_dict['schema_name']
    table_name = inputs_dict['table_name']
    """
    Given a list of file keys within the staging directory, grab the key at the
    top of the list, load it from s3 bucket, parse and transform the contents,
    then commit the data in to an SQL table. Finally, remove the file key from
    the list.

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
    fnames_list : list
        List of strings, the keys of all files within staging directory. Note 
        that the key strings also contain directory information.
    schema_name : str
        Name of the SQL table schema for data to be loaded in to.
    table_name : str
        Name of the SQL table name for data to be loaded in to.

       
    Returns
    -------
    dict
        Passes through all input values, with the top file key removed from 
        fnames_list and instead passed explicitly as fname_processed. The
        dictionary also explicitly passes the number of files left in the list
        which still need to be processed, as well as a boolean value indicating
        whether or not the loading/transforming/writing to SQL procedure worked
        without an error.
        
        Whether or not there was an error in the procedure determines how the 
        AWS step function procedes: the processed file will be moved in to one
        of two directories depending.
        
    """

    # Default is that there was no error in the procedure
    error = False
    try:
        # Retrieve one file from list, to be added to dbase, then remove key
        top_fname = fnames_list[0]
        fnames_list.remove(top_fname)
        
        # Import data from file
        s3 = boto3.resource('s3')
        data_obj = s3.Object(bucket_name, top_fname)
        data_fetched = data_obj.get()
        data_pickle = data_fetched['Body'].read()
        data = pkl.loads(data_pickle)

        # Transform data to fit table schema
        transformed_data = transform_all_data_dicts_for_sql_schema(top_fname,
                                                                   data,
                                                                   )
        # Create Posgresql commands to insert data
        insert_str, values = convert_dict_to_insert_statement(schema_name,
                                                              table_name,
                                                              transformed_data,
                                                              conn
                                                             )
        # Open cursor within connection
        cur = conn.cursor()
        
        # Create schema, if it doesn't exist
        create_schema = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
        cur.execute(create_schema)
    
        # Create table, if it doesn't exist
        create_table  = create_table_string(schema_name, table_name)
        cur.execute(create_table)
        
        # Execute insert command and commit to table
        psycopg2.extras.execute_values(cur, insert_str, values)
        conn.commit()
        
        # Close cursor
        cur.close()
    
    # If there is an error, change boolean and print output
    except Exception as e:
        print('Exception type: ', type(e))
        print('Exception args: ', e.args)
        print('Exception ', e)
        error = True
    
    
    return_dict = {'bucket_name': bucket_name,
                   'staging_dir': staging_dir,
                   'completed_dir': completed_dir,
                   'error_dir': error_dir,
                   'schema_name': schema_name,
                   'table_name': table_name,
                   'fnames_list': fnames_list,
                   'fname_processed': top_fname
                   }
    
    return {
            'statusCode': 200,
            'num_names_left': len(fnames_list),
            'error': error,
            'body': json.dumps(return_dict)
            }




def convert_dict_to_insert_statement(schema_name, table_name, data_dict, conn):
    """
    Given a python dictionary, build an insert statement that inserts all dict
    data in to SQL table.

    Parameters
    ----------
    schema_name : str
        The name of the sql schema data is to be inserted in to.
    table_name : str
        The name of the sql table within the schema which data is to be 
        inserted in to.
    data_dict : dict
        Python dictionary containing all data to be inserted. The keys each 
        correspond to a particular instance (one row of the table), the values
        are themselves dictionaries containing all the instance data. The keys
        of the inside dict correspond to columns within the table.
    conn : psycopg2 object
        Contains connection and authentication info for Postgresql table.

    Returns
    -------
    insert_str : str
        A string outlining the SQL insert command to be used, has schema.table
        as well as all column names.
    values : str
        The actual values to be inserted in to the table.

    """
    
    # Get a Reddit submission ID to access one of the internal data-dicts
    a_key = list(data_dict.keys())[0]
    
    # Use the internal dict (containing all instance/Reddit post data) to 
    #   build the set of column names for the table
    columns = data_dict[a_key].keys()
    
    # Construct the insert statement
    insert_str = '''INSERT INTO {}.{} ({}) VALUES %s 
                    ON CONFLICT (ID) DO NOTHING'''.format(schema_name,
                                                          table_name,
                                                          ','.join(columns)
                                                         )
    
    # Unpack the values to be inserted
    values = [[value for value in data_dict[key].values()] 
              for key in data_dict.keys()]
    
    return insert_str, values




