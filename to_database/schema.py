# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 11:05:33 2021

@author: Dillo
"""



"""
The schema to be used for the SQL table
"""
columns = [
            'Title                      varchar(301),',
            'Author                     varchar(31),',
            'ID                         varchar(30) PRIMARY KEY,',
            'Gilded                     integer,',
            'Upvotes                    integer,',
            'Upvote_ratio               real,',
            'Post_time                  integer,',
            'Views                      integer,',
            'Discussion_type            varchar(50),',
            'Distinguished              boolean,',
            'Contest_mode               boolean,',
            'Content_categories         varchar(50),',
            'Edited                     boolean,',
            'Hidden                     boolean,',
            'Crosspostable              boolean,',
            'Crossposts                 integer,',
            'Meta                       boolean,',
            'OC                         boolean,',
            'Reddit_media               boolean,',
            'Robot_indexable            boolean,',
            'Selfpost                   boolean,',
            'Video                      boolean,',
            'Likes                      integer,',
            'Comments                   integer,',
            'Adult_content              boolean,',
            'Subreddit                  varchar(50),',
            'Created_utc                double precision,',
            'Post_age                   real,',
            'Upvote_rate                real,',
            'Comment_rate               real,',
            'avg_up_rate                real,',
            'std_up_rate                real,',
            'gild_rate	                real,',
            'distinguished_rate         real,',
            'op_comment_rate            real,',
            'premium_auth_rate          real,',
            'Silver_awarded             integer,',
            'Gold_awarded               integer,',
            'Platinum_awarded           integer,',
            'How_sorted                 varchar(15),',
	        'Initial_silver	    	    integer,',
	        'Initial_gold		        integer,',
	        'Initial_platinum		    integer,',
            'Final_upvotes              integer,',
            'Final_num_comments         integer'
          ]




def create_table_string(schema_name, table_name):
    """
    Create the string which defines execution to build the table in Postgresql,
    table only created if it doesn't already exist within the schema.

    Parameters
    ----------
    schema_name : str
        Name of the schema in which the table is to be built.
    table_name : str
        Name of the table which will be created with above schema.

    Returns
    -------
    string: str
        String to be executed to build table.

    """
    
    string = 'CREATE TABLE IF NOT EXISTS '
    string += schema_name + '.' + table_name + '('
    for feature in columns:
        string += feature + ' '
    string = string.rstrip()
    string += ');'
    
    return string




if __name__ == '__main__':
    columns = ['col1 integer PRIMARY KEY, col2 varchar(30)']
    print('Sample output:', create_table_string('schema_name', 'table_name') )
    pass