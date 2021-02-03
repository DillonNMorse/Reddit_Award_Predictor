# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 13:12:02 2021

@author: Dillo
"""

class get_db_auth:
    """
    Convert a txt file containing AWS dbase auth info in to an authorization
    object containing necessary keys, etc.

    Parameters
    ----------
    db_auth_file : str
        Filepath of .txt file containing Reddit keys, see sample_auth.txt

    Returns
    -------
    auth : auth_object
        Authorization object with properites needed to access the SQL table.
    """

    def __init__(self, reddit_auth_file):

        lines = reddit_auth_file['Body'].read().decode('utf-8').split('\n')

        for k, line in enumerate(lines):

            if len(line.split('=')) < 2:
                pass
            else:
                variable_name = line.split('=')[0].strip()
                variable_value = line.split('=')[1].strip()

                if variable_name == 'ENDPOINT':
                    self.ENDPOINT = variable_value
                elif variable_name == 'PORT':
                    self.PORT = variable_value
                elif variable_name == 'USR':
                    self.USR = variable_value      
                elif variable_name == 'REGION':
                    self.REGION = variable_value 
                elif variable_name == 'DBNAME':
                    self.DBNAME = variable_value 
                elif variable_name == 'DBPWD':
                    self.DBPWD = variable_value                     
