# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 12:40:40 2021

@author: Dillo
"""

from datetime import datetime as dt

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import pandas as pd

from sklearn.metrics import precision_score, recall_score, roc_curve, auc
from sklearn.metrics import precision_recall_curve



def handle_unique_columns(df):
    
    num_gilds = df['gold_awarded'] + df['platinum_awarded']
    gilded = num_gilds.apply(lambda x: 1 if x > 0 else 0)
    
    weekday = df['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).weekday() )
    hour = df['created_utc'].apply(lambda x: dt.utcfromtimestamp(x).hour )
    
    scrape_time = [ID[-4:] for ID in df['id']]
    
    return gilded, weekday, hour, scrape_time




def get_all_useless_cols(df):
    
    useless_cols = ['id', 'created_utc', 'gold_awarded', 'platinum_awarded']
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
    
    
    
    
def make_evaluation_plots(clf, X_train, y_train, X_test, y_test, thresh = 0.5):
    
    gild_train_probas = [x[1] for x in clf.predict_proba(X_train)]    
    precision_train, recall_train, thresh_pr_train =\
        precision_recall_curve(y_train, gild_train_probas)
    fpr_train, tpr_train, thresh_roc_train =\
        roc_curve(y_train, gild_train_probas)
    predictions_train = [True if x > thresh else False 
                         for x in gild_train_probas
                         ]  

    roc_auc_score_train = auc(fpr_train, tpr_train)
    pr_auc_train = auc(recall_train, precision_train)
    prec_train = precision_score(y_train, predictions_train)
    reca_train = recall_score(y_train, predictions_train)

    ratio_train = sum(predictions_train)/len(predictions_train)
    train_stats = ('''Threshold: {}\n        Ratio: {:.2f}%\n\n Precision: {:.1f}%\n      Recall: {:.1f}%\n         AUC: {:.3f}'''
                   .format(thresh,
                           ratio_train*100,
                           prec_train*100,
                           reca_train*100,
                           pr_auc_train
                           )
                  )
    no_skill_train = y_train.sum()/len(y_train)

    
    
    
    gild_test_probas = [x[1] for x in clf.predict_proba(X_test)]    
    precision_test, recall_test, thresh_pr_test =\
        precision_recall_curve(y_test, gild_test_probas)
    fpr_test, tpr_test, thresh_roc_test = roc_curve(y_test, gild_test_probas)
    predictions_test = [True if x > thresh else False 
                        for x in gild_test_probas
                        ]
    
    roc_auc_score_test = auc(fpr_test, tpr_test)
    pr_auc_test = auc(recall_test, precision_test)
    prec_test = precision_score(y_test, predictions_test)
    reca_test = recall_score(y_test, predictions_test)

    ratio_test = sum(predictions_test)/len(predictions_test)
    test_stats = ('''Threshold: {}\n        Ratio: {:.2f}%\n\n Precision: {:.1f}%\n      Recall: {:.1f}%\n         AUC: {:.3f}'''
                   .format(thresh,
                           ratio_test*100,
                           prec_test*100,
                           reca_test*100,
                           pr_auc_test
                           )
                  )
    no_skill_test = y_test.sum()/len(y_test)




    fig, ((ax1, ax2, ax3),(ax4, ax5, ax6)) =plt.subplots(2,3,figsize = (15,10))
    plt.subplots_adjust(wspace=None, hspace=0.4)
    
    ax1.hist(gild_train_probas)
    ax1.set_yscale('log')
    ax1.set_xlabel('Predicted Probability of Being Gilded')
    ax1.set_ylabel('Number of Posts')



    points = np.array([recall_train[:-1], precision_train[:-1]]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)
    norm = plt.Normalize(thresh_pr_train.min(), thresh_pr_train.max())
    lc = LineCollection(segments, cmap='viridis', norm=norm)
    # Set the values used for colormapping
    lc.set_array(thresh_pr_train)
    lc.set_linewidth(4)
    line = ax2.add_collection(lc)
    fig.colorbar(line, ax=ax2)
# =============================================================================
#     pr_train = ax2.scatter(recall_train[:-1],
#                            precision_train[:-1],
#                            c = thresh_pr_train,
#                            label = ('Area under curve: {:.2f}'
#                                     .format(pr_auc_train)
#                                     )
#                           )
# =============================================================================
    ax2.plot([0, 1],
             [no_skill_train, no_skill_train],
             color = 'navy', lw = 3, linestyle='--',
             label='No Skill'
             )
    ax2.text(0.48, 0.65,
             train_stats,
             bbox={'facecolor': 'steelblue', 'alpha': 0.1, 'pad': 10},
             transform = ax2.transAxes
             )
    ax2.set_xlabel('Recall')
    ax2.set_ylabel('Precision')
    #plt.colorbar(pr_train, ax=ax2)
    #ax2.legend(loc = 'upper right')
    ax2.set_title('Training Data', fontsize = 18)
    
    roc_train = ax3.plot(fpr_train,
                         tpr_train,
                         lw = 3,
                         color='darkorange',
                         label = 'Area under ROC curve: {:.2f}'
                                  .format(roc_auc_score_train)
                        )
    ax3.legend(loc="lower right")
    ax3.plot([0, 1], [0, 1], color='navy', lw=3, linestyle='--')
    ax3.set_xlabel('False Positive Rate')
    ax3.set_ylabel('True Positive Rate')

    
    
    
    
    
    ax4.hist(gild_test_probas)
    ax4.set_yscale('log')
    ax4.set_xlabel('Predicted Probability of Being Gilded')
    ax4.set_ylabel('Number of Posts')
    

    points = np.array([recall_test[:-1], precision_test[:-1]]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)
    norm = plt.Normalize(thresh_pr_test.min(), thresh_pr_test.max())
    lc = LineCollection(segments, cmap='viridis', norm=norm)
    # Set the values used for colormapping
    lc.set_array(thresh_pr_test)
    lc.set_linewidth(4)
    line = ax5.add_collection(lc)
    fig.colorbar(line, ax=ax5)



# =============================================================================
#     pr_test = ax5.plot(recall_test[:-1],
#                            precision_test[:-1],
#                            c = thresh_pr_test,
#                            label = ('Area under curve: {:.2f}'
#                                     .format(pr_auc_test)
#                                     )
#                           )
# =============================================================================
    ax5.plot([0, 1],
         [no_skill_test, no_skill_test],
         color = 'navy', lw = 3, linestyle='--',
         label='No Skill'
         )
    ax5.text(0.48, 0.65,
             test_stats,
             bbox={'facecolor': 'steelblue', 'alpha': 0.1, 'pad': 10},
             transform = ax5.transAxes
             )
    ax5.set_xlabel('Recall')
    ax5.set_ylabel('Precision')
    #plt.colorbar(pr_test, ax=ax5)
    #ax5.legend(loc = 'upper right')
    ax5.set_title('Test Data', fontsize = 18)
    
    roc_test = ax6.plot(fpr_test,
                         tpr_test,
                         lw = 3,
                         color='darkorange',
                         label = ('Area under ROC curve: {:.2f}'
                                  .format(roc_auc_score_test)
                                  )
                        )
    ax6.legend(loc="lower right")
    ax6.plot([0, 1], [0, 1], color='navy', lw=3, linestyle='--')
    ax6.set_xlabel('False Positive Rate')
    ax6.set_ylabel('True Positive Rate')
    
    plt.show()    
    
    return