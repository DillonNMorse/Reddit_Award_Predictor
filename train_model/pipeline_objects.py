# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 13:54:13 2021

@author: Dillo
"""

from datetime import datetime as dt
import pickle
import os

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import boto3

from sklearn.base import BaseEstimator, TransformerMixin, ClassifierMixin
from sklearn.preprocessing import OneHotEncoder



# =============================================================================
# GetData
# PrepData
# MyTargetEncoder
# MultipurposeEncoder
# MyProbBuilder
# DropFeatures
# =============================================================================

def build_dbase_info_dict(info_txt_file_str):
    """
    Load txt file in to dict, to pass to GetData init. 

    Parameters
    ----------
    info_txt_file_str : str
        The fullpath of the txt file containing AWS database auth and info.

    Returns
    -------
    database_info : dict
        Dictionary, keys are auth/info variable names.

    """
    
    with open(info_txt_file_str, 'r') as f:
        database_info = {}
        for line in f:
            var_name = line.split('=')[0].strip()
            var_value = line.split('=')[1].strip()
            database_info[var_name] = var_value

    return database_info




class GetData():
    
    def __init__(self, database_info_dict):
        
        self.database_info_dict = database_info_dict
        self.cluster_arn = database_info_dict['cluster_arn']
        self.secret_arn = database_info_dict['secret_arn']
        self.database = database_info_dict['database']
        self.schema = database_info_dict['schema']
        self.tablename = database_info_dict['tablename']



    def make_sql_query(self, sql):
        """
        Query the database with the given sql string

        Parameters
        ----------
        sql : string
            String containing sql query.

        Returns
        -------
        response : dict
            Dictionary from database containing both the query response and the
            metadata for the query.

        """
        
        rdsData = boto3.client('rds-data')

        response = rdsData.execute_statement(resourceArn =self.cluster_arn,
                                             includeResultMetadata =  True,
                                             secretArn = self.secret_arn,
                                             database = self.database,
                                             schema = self.schema,
                                             sql = sql
                                             )    
        return response     




    def define_features(self):
        """
        Define and return the columns to be included in the sql query SELECT 
        statement

        Returns
        -------
        features : str
            String containing all column names in the SELECT statement.

        """
        
        features = '''contest_mode, edited, adult_content, oc, 
                      content_categories, reddit_media, selfpost, video,
                      subreddit, how_sorted, distinguished, upvotes,
                      upvote_ratio, crossposts, comments,  post_age,
                      upvote_rate,  comment_rate, avg_up_rate, std_up_rate,
                      gild_rate, distinguished_rate, op_comment_rate,
                      premium_auth_rate, initial_silver, id, created_utc, 
                      gold_awarded, platinum_awarded, final_upvotes,
                      final_num_comments, title
                      '''
                      
        return features




    def get_scrape_metadata(self):
        """
        Retrieve list of all scrape-metadata identifiers, this populates a list
        on which the entire table can be segmented so as to retrieve the
        data in < 1mb chunks.

        Returns
        -------
        list
            List of strings, each corresponding to a separate scrape event 
            within AWS. Every instance is labelled with one such scrape
            metadata tag. 

        """
        
        scrape_metadata_query = '''SELECT DISTINCT SUBSTRING(id, 8) 
                                    FROM reddit.RedditPostsData'''
        response = self.make_sql_query(scrape_metadata_query)
        
        return [list(line[0].values())[0] for line in response['records']] 




    def build_get_data_query(self, set_of_ids_to_query):
        """
        Build the full query string when calling the sql database.

        Parameters
        ----------
        set_of_ids_to_query : list
            List of strings, the scrape metadata tags to be included in this
            particular query. This acts as a limit to the number of rows that
            are queried at once in order to limit the response size.

        Returns
        -------
        SQL_query_string : str
            String containing the full sql query to be made.

        """
        
        SQL_query_string = ("""SELECT {} 
                               FROM {}.{} 
                               WHERE SUBSTRING(id,8) in {}"""
                               .format(self.define_features(),
                                       self.schema,
                                       self.tablename,
                                       tuple(set_of_ids_to_query)
                                       ))
        return SQL_query_string




    def get_all_data(self, num_in_query_block = 16):
        """
        Pulls all current data database, as controlled by the features() string
        which is input in to the SELECT statement. Iterates over all sets of
        scrape metadata tags, ensuring that each individual call stays beneath
        the AWS response size.

        Parameters
        ----------
        num_in_query_block : int, default=16
            The number of unique scrape metadata tags to be included in each
            individual query. More tags means fewer queries, but more tags also
            increases the response size. Set largely by trial and error. If 
            AWS (via boto3) returns an exception, drop this value.

        Returns
        -------
        df : pandas.DataFrame
            A dataframe containing all the data queried.

        """
        
        scrape_metadata = self.get_scrape_metadata()
        
        num_of_ids = len(scrape_metadata)
        query_block_idxs = np.arange(0, num_of_ids, num_in_query_block)
        
        
        build_query = self.build_get_data_query
        make_query = self.make_sql_query
        
        all_data = []
        for block_num, lower_idx in enumerate(query_block_idxs):
            upper_idx = lower_idx + num_in_query_block
            set_of_ids_to_query = scrape_metadata[lower_idx:upper_idx]
            
            sql = build_query(set_of_ids_to_query)
            response = make_query(sql)
            new_data = [[list(feature.values())[0] for feature in record] 
                        for record in response['records']
                        ]
            all_data += new_data
            
            if block_num == 0:
                col_names = [column['name'] for column 
                             in response['columnMetadata']
                             ]
            if (block_num + 1)%5 == 0:
                print('Block {} of {} completed.'.format(block_num + 1,
                                                         len(query_block_idxs)
                                                       ))
                
        df = pd.DataFrame(all_data)
        df.columns = col_names
    
        return df




    def build_fstring(self, data_directory):
        """
        Build a string for saving/loading a particular dataframe from disk,
        includes the data that the query was made as well as the directory
        where the data should be saved to / loaded from.

        Parameters
        ----------
        data_directory : str
            The directory where data is located, relative to current working
            directory.

        Returns
        -------
        fstring : str
            A string with both the directory and unique filename.

        """
        
        day = dt.today().day
        month = dt.today().month
        year = dt.today().year

        dt_str = 'reddit_data_{}-{}-{}'.format(month, day, year)
        
        fstring = os.path.join(data_directory, dt_str)       
          
        return fstring




    def data_to_disk(self, data_directory = os.path.join('.','data'),
                     num_in_query_block = 16
                     ):
        """
        Pull data from database and save data to disk.

        Parameters
        ----------
        data_directory : str, default='./data'
            The directory where the data is to be saved, relative to current
            working directory.
        num_in_query_block : int, default=16
            The number of unique scrape metadata tags to be included in each
            individual query. More tags means fewer queries, but more tags also
            increases the response size. Set largely by trial and error. If 
            AWS (via boto3) returns an exception, drop this value.

        Returns
        -------
        None.

        """
        
        df = self.get_all_data(num_in_query_block)
        fstring = self.build_fstring(data_directory)
        df.to_pickle(fstring)
        
        return None




    def load_data(self, data_directory = os.path.join('.','data'),
                     num_in_query_block = 16, fstring = ''
                     ):
        """
        Load data if it exists, otherwise query the database and save results
        to disk, then load.

        Parameters
        ----------
        data_directory : str, default='./data'
            DESCRIPTION. The default is os.path.join('.','data').
        num_in_query_block : int, default=16
            The number of unique scrape metadata tags to be included in each
            individual query. More tags means fewer queries, but more tags also
            increases the response size. Set largely by trial and error. If 
            AWS (via boto3) returns an exception, drop this value.
        fstring : str, default=''
            An alternate fstring (directory and filename) to be loaded, if it
            exists. Currently cannot build the file unless it corresponds to 
            todays data.

        Raises
        ------
        Exception
            Try 3 times to load/build the data, if not return exception.

        Returns
        -------
        df : pandas.DataFrame
            A dataframe containing all data in database, limited only by the
            features used in SELECT statement.

        """
        if len(fstring) == 0: 
            fstring = self.build_fstring(data_directory)       
        
        file_loaded = False
        counter = 0
        while not file_loaded:
            try:
               df = pd.read_pickle(fstring)
               file_loaded = True
            except:
                print('Attempting to query database to build data file.')    
                self.data_to_disk()
                counter += 1
            
            if counter >= 3:
                raise Exception('File cannot be loaded')
           
        return df







class PrepData(BaseEstimator, TransformerMixin):


    def __init__(self, drop_feats = [], drop_subs = ['blog']):
        """
        Sets up the class for data preparation

        Parameters
        ----------
        drop_feats : list, default=[]
            List of strings, the features which are to be dropped. Can set as
            False to completely bypass any dropping of featrues whatsoever.
            
        drop_subs : list, default=['blog']
            List of strings, the subreddits not to be included in the analysis.
   
        
        Returns
        -------
        None.

        """
        
        self.drop_feats = drop_feats
        self.drop_subs = drop_subs




    def fit(self, X, y = None):
        """
        Compute any necessary transformation parameters based on input data.

        Parameters
        ----------
        X : dataframe
            The training feature data, size: n-by-m where n is the number of
            entries and m is the number of features.
        y : series, optional
            The training target data, size: n-by-1 where n is the number of 
            entries. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        # Prepare a full list of features to drop, including not only those
        #   explicitly listed in *some_useless_cols* but also by looking for
        #   any features which have a cardinality of one.
        # Can set *drop_feats = False* to completely bypass this step.
        if type(self.drop_feats) == type([]):
            some_useless_cols = ['created_utc', 'gold_awarded', 
                                 'platinum_awarded', 'scrape_time'
                                ]
            for feature in self.drop_feats:
                some_useless_cols.append(feature)
            
            self.all_useless_cols = self.get_all_useless_cols(X,
                                                              some_useless_cols
                                                              )
            # Sometimes *gilded* is picked up by this procedure, not sure why.
            #   Need to manually exclude it.
            if 'gilded' in self.all_useless_cols:
                self.all_useless_cols.remove('gilded')

        return self



    def remove_useless_subreddits(self, df, subs_to_remove = []):
        """
        Remove from the dataframe all entries coming from the indicated 
        subreddits

        Parameters
        ----------
        df : dataframe
            The data which is to have entries removed.
        subs_to_remove : list, optional
            List of strings, those subreddits which are to be removed from the 
            analysis. The default is [].

        Returns
        -------
        df : dataframe
            The original data with selected entries removed.

        """
        n = df.shape[0]
        bool_mask = pd.Series([True]*n, index = df.index)

        for subreddit in subs_to_remove:
            bool_mask &= df['subreddit'] != subreddit
        return df[bool_mask].reset_index(drop = True)    




    def get_all_useless_cols(self, X, some_cols):
        """
        Determine those features with cardinality one, append them to the list
        of features which are to be dropped.

        Parameters
        ----------
        X : dataframe
            The training data which is to be used to count cardinality.
        some_cols : list
            List of strings, those features which were explicitly listed for 
            removal.

        Returns
        -------
        useless_cols : list
            List of strings, all features which are to be removed.

        """
        useless_cols = some_cols
        for col in X.columns:
            try:
                num_unique_entries = len(X[col].unique())
                if num_unique_entries == 1:
                    useless_cols.append(col)
            except:
                pass        
        return useless_cols




    def transform(self, X, y = None):
        """
        Perform transformation on input data

        Parameters
        ----------
        X : dataframe
            The training feature data, size: n-by-m where n is the number of
            entries and m is the number of features.
        y : series, optional
            The training target data, size: n-by-1 where n is the number of 
            entries. The default is None.

        Returns
        -------
        df : dataframe
            The transformed data, ready to be fit.

        """
        
        
        df = X.copy()
        
        # Build out some new features
        num_gilds = X['gold_awarded'] + X['platinum_awarded']
        gilded = num_gilds.apply(lambda x: 1 if x > 0 else 0)

        weekday = X['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).weekday() )
        hour = X['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).hour )

        scrape_time = [ID[-4:] for ID in X['id']]

        df['gilded'] = gilded.astype('bool')
        df['weekday'] = weekday
        df['post_hour'] = hour
        df['scrape_time'] = scrape_time
        
        
        # Drop those features/subreddits indicated
        if type(self.drop_feats) == type([]):
            df.drop(columns = self.all_useless_cols, inplace = True)
        if self.drop_subs:
            df = self.remove_useless_subreddits(df, self.drop_subs)
            
        return df




class MyTargetEncoder(BaseEstimator, TransformerMixin):
    """
    Target-encode the passed feature columns. Allow the encoding values to be
    smoothed in order to minimize over-fitting of relatively rare instances.
    
    Set the weight larger to increase the smoothing (bringing all encoding 
    values nearer to the overall-target average), this dispraporionately
    affects category values which have a total number of occurences less than
    or near the weight value - those values with a large number of occurences
    are less affected.
    
    Setting weight to 0 is equivalent to 'vanilla' target encoding.
    """
    
    def __init__(self, how = 'vanilla', weight = 1):
        
        self.how = how
        self.weight = weight
        
    def fit(self, X, y):
        
        df = X.join(y)
        
        if self.how == 'additive_smoothing':
            overall_mean = y.mean()
        
        self.all_means = {}
        self.features = list(X.columns)
        for feature in self.features:
            
            feature_replace_dict = {}
            agg = df.groupby(feature).agg(['count', 'mean']).iloc[:,-2:]
            means = agg.iloc[:,-1]
            count = agg.iloc[:,-2]

            if self.how == 'additive_smoothing':
                new_means = [(count[j]*means[j] + self.weight*overall_mean)/
                             (count[j] + self.weight) for j, _ 
                             in enumerate(means)
                            ]
                means_index = means.index
                means = pd.Series(new_means, index = means_index)
            
            feature_replace_dict = {}
            for value in means.index:
                feature_replace_dict[value] = means.loc[value]
                
            self.all_means[feature] = feature_replace_dict
            
        return self
        
    
    def transform(self, X, y = None):
        
        df = X.copy()
        
        for feature in self.features:
            df[feature] = X[feature].replace(self.all_means[feature])
        
        return df
    
    




class MultipurposeEncoder(BaseEstimator, TransformerMixin):
    """
    Encode the passed columns, allow user to specify which columns (if any)
    should be one-hot-encoded and which should be target-encoded.
    """
    
    def __init__(self, ohe_feats = [], target_feats = [],
                 target_how = 'vanilla', target_weight = 1,
                 ):
        
        self.ohe_feats = ohe_feats
        self.target_feats = target_feats
        self.target_how = target_how
        self.target_weight = target_weight

   
    def fit(self, X, y):
        
# =============================================================================
#         if len(self.target_feat_name) > 0:
#             target = X[self.target_feat_name]
#         else:
#             if type(y) == type(None):
#                 raise Exception('''Must specify a target, either with a feature
#                                    name or by explicitly passing y''')
#             else:
#                 target = y
# =============================================================================
        target = y        

        if len(self.ohe_feats) > 0:
            self.ohenc = OneHotEncoder(drop = 'if_binary')
            self.ohenc.fit(X[self.ohe_feats])
        if len(self.target_feats) > 0:
            self.target_enc = MyTargetEncoder(how = self.target_how,
                                              weight = self.target_weight
                                             )
            self.target_enc.fit(X[self.target_feats], target)

            self.replacement_dictionary = self.target_enc.all_means

        return self
    
    
    
    def transform(self, X, y = None):
        
        df = X.copy()
        
        if len(self.ohe_feats) > 0:  
            ohenc_array = self.ohenc.transform(X[self.ohe_feats])
            ohe_vars_names = self.ohenc.get_feature_names(self.ohe_feats)
            
            ohenc_df = pd.DataFrame.sparse.from_spmatrix(ohenc_array,
                                                         index = X.index
                                                         )
            ohenc_df.columns = ohe_vars_names
            
            df[self.ohe_feats] = ohenc_df
            
        if len(self.target_feats) > 0:
            target_df = self.target_enc.transform(X[self.target_feats])
        
            df[self.target_feats] = target_df
        
        #print('All columns: ', list(df.columns))
        return df




class MyProbBuilder(BaseEstimator, ClassifierMixin):
    """
    Bin data by given features and calculate the ratio of positive target
    values to all values within each bin, can be interpreted as analgous to the 
    probability of a post being positive-target given that it falls within a 
    particular bin.
    """
    
    def __init__(self, discrete_col_names = [], continuous_col_names = [], 
                 num_bins = 5
                 ):
        """
        Intialize.

        Parameters
        ----------
        discrete_col_names : list, default = []
            A list of string. The names of the discrete features that should be
            included.
        continuous_col_names : list, default = []
            A list of strings. The names of the continuous features that should
            be included.
        num_bins : int, default = 5
            The number of bins to divide the continuous data in to, each bin
            will have approximately the same number of entries.

        Returns
        -------
        None.

        """
        self.discrete_col_names = discrete_col_names
        self.continuous_col_names = continuous_col_names
        self.num_bins = num_bins




    def _histedges_equalN(self, x, num_bins):
        """
        Create bins for x with equal numbers of entries in each bin.

        Parameters
        ----------
        x : list or series
            The data which is to be binned.
        num_bins : int
            The number of bins to divide x in to.

        Returns
        -------
        bins : list
            List of floats, the bin-edges.

        """
        num_pts = len(x)
        bins = np.interp(np.linspace(0, num_pts, num_bins + 1),
                         np.arange(num_pts),
                         np.sort(x)
                        )
        return bins



        
    def fit(self, X, y):
        """
        Calculate the target ratio for each bin within X.

        Parameters
        ----------
        X : dataframe
            The Pandas dataframe containing the features of interst.
        y : series or list
            The target values, binary or boolean.

        Returns
        -------
        self

        """
        
        self.bins = {}
        
        num_entries = X.shape[0]
        df = X.copy()[self.discrete_col_names]
        for name in self.continuous_col_names:
            self.bins[name] = self._histedges_equalN(X[name], self.num_bins)
            df[name] = pd.cut(X[name],
                              bins = self.bins[name],
                              labels = range(self.num_bins)
                              )
        df['total'] = [1]*num_entries
        
        df['target'] = y
        
        df_binned = df.groupby(by = self.discrete_col_names 
                                   + self.continuous_col_names
                               ).sum()
        df_binned['target_fraction'] = df_binned.apply(lambda x: 
                                                       x['target']/x['total'] 
                                                       if x['total'] != 0 
                                                       else 0, 
                                                       axis = 1
                                                       )
        self.binned_data = df_binned
        
        zeros_df = self._fill_in_missing()
        
        if type(zeros_df) == type(None):
            pass
        else:
            self.binned_data = pd.concat([self.binned_data, zeros_df])
    
        return self




    def _vals_to_bin_num(self, vals_list):
        """
        Convert a list of values in to their coresponding bin number, used for 
        binning and counting frequency of target values.

        Parameters
        ----------
        vals_list : list
            List of floats, the feature values that are to be binned.

        Returns
        -------
        bin_numbers : list
            List of ints, corresponding to the bin in to which each value
            falls.

        """
        
        bin_numbers = []
        
        for idx, value in enumerate(vals_list):
            feature_name = self.continuous_col_names[idx]
            for k in range(1,self.num_bins):
                if value < self.bins[feature_name][k]:
                    break
                else:
                    pass
            bin_numbers.append(k-1)    
        
        if len(vals_list) == 1:
            bin_numbers = bin_numbers[0]
        
        return bin_numbers




    def _fill_in_missing(self):
        """
        Look for missing rows within the binned data, create a new dataframe of
        zeros with a row for each one missing, to be concateneted with the 
        larger set of binned data. Necessary for converting the target fraction
        vals in to an array for visualization.

        Returns
        -------
        zeros_df : dataframe
            All zeros, one row for each missing row within data to ensure that
            every combination of feature values is accounted for.

        """
        
        self.feat0_values  = self.binned_data.index.unique(level = 0)
        self.feat1_values  = self.binned_data.index.unique(level = 1)      
        
        zero_dataframes = []
        for feat0_val in self.feat0_values:
            for feat1_val in self.feat1_values:
                is_in_index = (feat0_val, feat1_val) in self.binned_data.index
                if not is_in_index:
                    idx = pd.MultiIndex.from_product([[feat0_val], 
                                                      [feat1_val]
                                                      ],
                                                     names = [self.binned_data
                                                                  .index
                                                                  .unique(level=0)
                                                                  .name,
                                                              self.binned_data
                                                                  .index
                                                                  .unique(level=1)
                                                                  .name,
                                                            ])
                    col = ['total', 'target', 'target_fraction']
                    temp_df = pd.DataFrame(0, idx, col)
                    zero_dataframes.append(temp_df)
        
        if len(zero_dataframes) > 0:
            zeros_df = pd.concat(zero_dataframes)
        else:
            zeros_df = None
            
        return zeros_df




    def predict_proba(self, X, y = None):
        """
        Use binned historical data to estimate the probability of each instance
        within X being gilded. This is a lookup from self.binned_data.

        Parameters
        ----------
        X : dataframe
            The dataframe containing features of interest.
        y : series, default = None
            The target values, not used.
            
        Raises
        ------
        Exception
            Exception if no feature names were passed in.

        Returns
        -------
        predict_proba : np.array
            Array of floats, column 1 containing the historical fraction of 
            gilded posts corresponding to each instance in X, columns zero 
            containing one minus this number (the fraction of non-gilded
            posts historically).

        """
        n_cont = len(self.continuous_col_names)
        n_disc = len(self.discrete_col_names)
        
        if n_cont > 0:
            value_to_bin = self._vals_to_bin_num
            cont_col_names = self.continuous_col_names
            X_cont_bin_nums = X[cont_col_names].apply(lambda x:
                                                      value_to_bin(x),
                                                      axis = 1
                                                      )
        
        if n_cont == 1:
            X_cont_bin_nums.name = cont_col_names[0]
        elif n_cont > 1:
            X_cont_bin_nums.columns = cont_col_names
        
        if n_disc > 0:
            X_discrete = X.copy()[self.discrete_col_names]
            
        if (n_disc > 0) & (n_cont > 0):
            X_all = pd.concat([X_discrete, X_cont_bin_nums], axis = 1)
        elif (n_disc > 0) & (n_cont == 0):
            X_all = X_discrete
        elif (n_cont > 0) & (n_disc == 0):
            X_all = X_cont_bin_nums
        elif (n_cont == 0) & (n_disc == 0):
            raise Exception('Nothing is happening! Give me some features.')
        
        binned_dat = self.binned_data
        self.probas = X_all.apply(lambda x: 
                             binned_dat.loc[tuple(x)]['target_fraction'], 
                             axis = 1
                             )
        
        return np.array([[1-x,x] for x in self.probas])




    def predict(self, X, y = None):
        """
        Make prediction for each instance within the dataframe X, based on 
        historical data. Use the fractions of historical posts with the same
        feature values which were gilded as a probability for a binomial 
        random prediction about whether instance is gilded or not.

        Parameters
        ----------
        X : dataframe
            Dataframe containing features of interest.
        y : series, default = None
            Target values, not used.

        Returns
        -------
        predictions : series
            Series of booleans, the predicted classes.

        """
        
        try:
            probabilities = [x[1] for x in self.probas]
        except:
            probabilities = [x[1] for x in self.predict_proba(X)]
        
        
        predictions = np.random.binomial(1, probabilities)
        
        return predictions




    def fracs_array(self):
        """
        Create array of frac values when two features are passed.

        Raises
        ------
        Exception
            Must intiate with exactly two features.

        Returns
        -------
        fracs_array : np.Array
            Array of floats, the fraction of all historical posts within a bin
            which had positive target values.

        """
        n_cont = len(self.continuous_col_names)
        n_disc = len(self.discrete_col_names)        
        
        if n_cont + n_disc != 2:
            raise Exception('Must provide two features to build an array.')
        

        self.feat0_values  = self.binned_data.index.unique(level = 0)
        n0 = len(self.feat0_values)
        self.feat1_values  = self.binned_data.index.unique(level = 1)      
        n1 = len(self.feat1_values)
        
        fracs_array = np.zeros([n0,n1])
        
        for vert_idx, feat0_value in enumerate(self.feat0_values):

            fracs_array[vert_idx, :] = (self.binned_data
                                            .loc[feat0_value]
                                            ['target_fraction']
                                            )
            
        return fracs_array





class AddNewFeatures(BaseEstimator, TransformerMixin):
    
    def __init__(self, feat_names = [], weight = 1):
        
        self.feat_names = feat_names
        self.weight = weight
        
    def fit(self, X, y):
        
        if 'Priors_Fractions' in self.feat_names:
            self.priors_clf = MyProbBuilder(['subreddit', 'post_hour'],
                                            ['upvote_rate'],
                                            5,
                                            ).fit(X, y)
        
        return self

    
    
    def transform(self, X, y = None):
        
        X_new = X.copy()
        
        if 'Priors_Fractions' in self.feat_names:  
            #probs = self.priors_clf.predict_proba(X)
            #X_new['prior_frac'] = [x[1] for x in probs]
            predictions = self.priors_clf.predict(X)
            X_new['priors_predict'] = predictions
            
            
        
        if 'Log_of_Features_Distance' in self.feat_names:
            axis_weights = [1, self.weight]
            min_upv = min([x if x > 0 else 10e5 for x in X['upvote_rate']])
            X_new['offset_upv']= X['upvote_rate'] + min_upv/10
            new_feat =  X_new.apply(lambda x: axis_weights[0]*np.log(x['offset_upv'])**2 
                                    + axis_weights[1]*np.log(x['post_age'])**2,
                                    axis = 1
                                    )
            X_new['log_feature'] = new_feat
            X_new.drop(columns = ['offset_upv'], inplace = True)
        return X_new
    
    
    
    
    
    
class DropFeatures():
    
    def __init__(self, features_to_drop):
        
        self.features_to_drop = features_to_drop
        
    
    def fit(self, X = None, y = None):
        return self
    
    def transform(self, X, y = None):
        
        X_new = X.copy()
        for feature in self.features_to_drop:
            try:
                X_new.drop(columns = [feature], inplace = True)
            except KeyError:
                pass

        return X_new
        
        
        