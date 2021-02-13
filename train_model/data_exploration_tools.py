# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 12:40:40 2021

@author: Dillo
"""

from datetime import datetime as dt

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd



def handle_unique_columns(df):
    
    num_gilds = df['gold_awarded'] + df['platinum_awarded']
    gilded = num_gilds.apply(lambda x: 1 if x > 0 else 0)
    
    weekday = df['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).weekday() )
    hour = df['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).hour )
    
    scrape_time = [ID[-4:] for ID in df['id']]
    
    return gilded, weekday, hour, scrape_time




def get_all_useless_cols(df):
    
    useless_cols = ['author', 'id', 'created_utc', 
                    'silver_awarded', 'gold_awarded', 'platinum_awarded',
                   'post_time',]
    for col in df.columns:
        try:
            num_unique_entries = len(df[col].unique())
            if num_unique_entries == 1:
                useless_cols.append(col)
        except:
            pass
        
    return useless_cols




def remove_useless_subreddits(df, subs_to_remove = []):
    
    n = df.shape[0]
    bool_mask = pd.Series([True]*n)
    
    for subreddit in subs_to_remove:
        bool_mask &= df['subreddit'] != subreddit
        
    return df[bool_mask]



categ_feats = ['title', 'contest_mode','edited', 'adult_content', 'oc','content_categories',
               'reddit_media', 'selfpost', 'video', 'subreddit', 'how_sorted', 'weekday', 
                'post_hour', 'distinguished', 'scrape_time'
              ]


numeric_feats = ['upvotes', 'upvote_ratio', 'crossposts',
                 'comments', 'post_age', 'upvote_rate',  'comment_rate', 
                 'avg_up_rate', 'std_up_rate', 'gild_rate', 'distinguished_rate', 
                 'op_comment_rate', 'premium_auth_rate', 'initial_silver',
                 'final_upvotes', 'final_num_comments'
                ]


target_feat = ['gilded']





class Categ_Analysis:
    
    def __init__(self, feature_column, target_column = None, ordinal = False):
        
        if not ordinal:
            self.feature_column = feature_column
        else:
            self.feature_column = feature_column.sort_values(ascending = True)
        
        self.target_column = target_column
        self.target_frac = False
        self.hist_dict = False
        self.target_nums = False

    def get_fracs(self, feat_values, hist_dict):
        
        target_frac = []
        target_nums = []
        for val in feat_values:
            target_num_in_bin = self.target_column[hist_dict[val]['bool_mask']].sum()
            target_frac_in_bin = target_num_in_bin/hist_dict[val]['num_in_bin']

            target_frac.append(target_frac_in_bin)
            target_nums.append(target_num_in_bin)
            
        return target_frac, target_nums
    
    
      
    def build_hist_dict(self, feat_values):

        hist_dict = {}
        for val in feat_values:
            val_bool_mask = self.feature_column == val
            num_in_bin = val_bool_mask.sum()
            hist_dict[val] = {}
            hist_dict[val]['num_in_bin'] = num_in_bin
            hist_dict[val]['bool_mask'] = val_bool_mask 
        
        return hist_dict
        

               
    def avg_list(self, L):
        return sum(L)/len(L)
    
    
     
    def variance_list(self, L):
        
        N = len(L)
        mean = self.avg_list(L)
        diff_means_squared = [(x - mean)**2 for x in L]
        std_dev = np.sqrt(sum(diff_means_squared)/N)
        
        stdem = std_dev/np.sqrt(N)
        
        return stdem
    
    
    
    def make_plot(self, title, xlabel, bin_names = False, conversion_fracs = False, error_bars = False, figsize = (10,10)):
        
        if conversion_fracs:
            if (self.target_column is None):
                raise Exception('Must supply a target column to get conversion fractions.')
        
        
        # Make distribution plt
        fig, ax = plt.subplots(1,1, figsize = figsize)
        
        feat_values = self.feature_column.unique()
        self.hist_dict = self.build_hist_dict(feat_values)
        
        num_vals = len(feat_values)
        bar_x_locs = np.arange(0, num_vals)
        heights = [val_dict['num_in_bin'] for val_dict in self.hist_dict.values()]
        
        ax.bar(bar_x_locs, heights, alpha = 0.7)
        ax.set_xticks(bar_x_locs)
        ax.set_xticklabels(self.hist_dict.keys())
        ax.set_ylabel('Total Number of Instances in Bin', fontsize = 14)
        
        ax.set_xlabel(xlabel, fontsize = 14)
        ax.set_title(title, fontsize = 18)
        
        if bin_names:
            ax.set_xticklabels(bin_names)
        
        # Build conversion rates       
        if conversion_fracs:         
            self.target_frac, self.target_nums = self.get_fracs(feat_values, self.hist_dict)
            frac_max = max(self.target_frac)
            legend_string = 'Fraction of Instances Equal To Target Value'
            
            ax2 = ax.twinx()
            ax2.plot(bar_x_locs, self.target_frac, 'ok', alpha = 0.7, label = legend_string)
            ax2.set_ylim([0,1.2*frac_max])
            ax2.legend(loc = 'upper right',)
           
        return fig        
             
    
    
    
    def get_chisq(self):
        from scipy.stats.distributions import chi2
        
        if not self.hist_dict:
            feat_values = self.feature_column.unique()
            self.hist_dict = self.build_hist_dict(feat_values)
        
        if not self.target_frac:
            feat_values = self.feature_column.unique()            
            self.target_frac, self.target_nums = self.get_fracs(feat_values, self.hist_dict)     
        
        mean_target_frac = self.target_column.mean()
        
        expected = [mean_target_frac*value_dict['num_in_bin'] for value_dict in self.hist_dict.values()]
        
        observed = self.target_nums

        chi_squared = 0
        for idx, _ in enumerate(self.target_frac):
            chi_squared += (observed[idx] - expected[idx])**2/expected[idx]
        
        DF = len(observed) - 1
        
        probability = chi2.sf(chi_squared, DF)

        return chi_squared, probability