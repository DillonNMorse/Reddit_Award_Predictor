# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 13:54:13 2021

@author: Dillo
"""

from datetime import datetime as dt

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin, ClassifierMixin
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import precision_score, recall_score, roc_curve, auc, precision_recall_curve
#from imblearn.over_sampling import SMOTE
#from imblearn.under_sampling import RandomUnderSampler
from matplotlib.collections import LineCollection
import numpy as np


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
            some_useless_cols = ['id', 'created_utc', 'gold_awarded', 
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
        
        
        return df




class MyProbBuilder():
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
        predict_proba : series
            Series of floats, the historical fraction of gilded posts
            corresponding to each instance in X.

        """
        n_cont = len(self.continuous_col_names)
        n_disc = len(self.discrete_col_names)
        
        if n_cont > 0:
            X_cont_bin_nums = X[self.continuous_col_names].apply(lambda x: 
                                                    self._vals_to_bin_num(x),
                                                    axis = 1
                                                                )
        
        if n_cont == 1:
            X_cont_bin_nums.name = self.continuous_col_names[0]
        elif n_cont > 1:
            X_cont_bin_nums.columns = self.continuous_col_names
        
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
        
        self.predict_proba = X_all.apply(lambda x: 
                             self.binned_data.loc[tuple(x)]['target_fraction'], 
                             axis = 1
                             )
        
        return self.predict_proba




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
        
        if self.predict_proba:
            pass
        else:
            self.probas(X)
        
        
        predictions = np.random.binomial(1, self.predict_proba)
        
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