# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 11:14:07 2021

@author: Dillo
"""

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score
from xgboost.sklearn import XGBClassifier

from pipeline_objects import MultipurposeEncoder, AddNewFeatures
from feats_from_nlp import My_tfidf


class FeatureSelection():
    
    def __init__(self, min_features, oh_encode_feats, target_encode_feats,
                 scoring = 'average_precision',  cv_folds = 5, verbose = 0
                 ):
        
        self.min_features = min_features
        self.oh_encode_feats = oh_encode_feats
        self.target_encode_feats = target_encode_feats
        self.scoring = scoring
        self.cv_folds = cv_folds
        self.verbose = verbose



    def _build_pipeline(self, pipeline_args, dropped_feats):
        
        target_feats, ohe_feats, num_tfidf_feats, new_feats, = pipeline_args

        add_features = AddNewFeatures(feat_names = new_feats,
                                      weight = 3,
                                     )
        remove_features = RemoveFeatures(feats_to_remove = dropped_feats)
        encode_categorical = MultipurposeEncoder(ohe_feats = ohe_feats,
                                                 target_feats = target_feats,
                                                )
        encode_tfidf = My_tfidf(target_column_name = 'title',
                                tfidf_num_feats = num_tfidf_feats,
                                num_pca_vars = 0
                               )
        scale_features = StandardScaler()
        classify = XGBClassifier(eta = 0.25,
                                 gamma = 2,
                                 min_child_weight = 5,
                                 max_depth = 3,
                                 max_delta_step = 10,
                                 subsample = 1,
                                 sampling_method = 'uniform',
                                 reg_lambda = 10,
                                 alpha = 0,
                                 scale_pos_weight = 2,
                                 eval_metric = 'aucpr',
                                 use_label_encoder = False,
                                 verbosity = 0
                                )
        
        
        pipeline = Pipeline([('add_features', add_features),
                             ('remove_features', remove_features),
                             ('encode_categorical', encode_categorical),
                             ('encode_tfidf', encode_tfidf),
                             ('scale_features', scale_features),
                             ('classify', classify)
                            ])
        
        return pipeline




    def _prepare_data(self, feature_list):
        
        target_feats = [feature for feature in self.target_encode_feats 
                        if feature in feature_list
                       ]
        ohe_feats = [feature for feature in self.oh_encode_feats 
                     if feature in feature_list
                    ]
        
        num_tfidf_feats = 0
        if 'title' in feature_list:
            num_tfidf_feats = 500
            

        new_feats = []
        if 'log_upvote_rate' in feature_list:
            new_feats.append('Log_of_features_Distance')
        
        return target_feats, ohe_feats, num_tfidf_feats, new_feats




    def _fit_with_these_features(self, X, y, kept_feature_list, dropped_feats):
        
        pipeline_args = self._prepare_data(kept_feature_list)
        pipeline = self._build_pipeline(pipeline_args, dropped_feats)
        score = cross_val_score(pipeline,
                                X, y,
                                scoring = self.scoring,
                                cv = self.cv_folds,
                                n_jobs = -1,
                                verbose = 0,
                                error_score = 'raise'
                            )
        
        return score




    def _find_next_feature_to_drop(self, X, y, current_features, removed_features):
    
        max_score = 0
        scores_std = 0
        dropped_feature = ''
        for feature in current_features:
            
            kept_feats = current_features.copy()
            dropped_feats = removed_features.copy()
            kept_feats.remove(feature)
            dropped_feats.append(feature)
            
            if self.verbose == 2:
                print('\n\tTrying with {} removed.'.format(feature))
# =============================================================================
#             try:
#                 X_temporary = X.drop(columns = [feature])
#             except KeyError:
#                 X_temporary = X
# =============================================================================
            feature_scores = self._fit_with_these_features(X,
                                                           y,
                                                           kept_feats,
                                                           dropped_feats
                                                          )
            if self.verbose == 2:
                print('\tResulted in score of {:.4f}.'.format(feature_scores.mean()))
            if feature_scores.mean() > max_score:
                max_score = feature_scores.mean()
                scores_std = feature_scores.std()
                dropped_feature = feature
            
        new_current_features = current_features.copy()
        new_current_features.remove(dropped_feature)
    
        return new_current_features, dropped_feature, max_score, scores_std




    def select_features(self, X, y, all_features):
        
        n_features = len(all_features)

        # Hold all data
        scores = {}

        # Build baseline by including all features
        feature_scores = self._fit_with_these_features(X, y, all_features, [])
        scores[n_features] = {'dropped': 'None',
                              'scores_mean': feature_scores.mean(),
                              'scores_std': feature_scores.std()
                             }
        print('Initial fit with all features score: {:.4f}'
              .format(feature_scores.mean())
             )
        

        # Iterate by removing one feature on each pass.
        current_features = list(all_features.copy())
        removed_features = []
        #X_current = X.copy()
        #dropped_log_upvote_rate = False
        while len(current_features) > self.min_features:
            
            if self.verbose >= 1:
                print('Currently fitting with {} features'.format(n_features))
            if self.verbose >= 1.5:
                print('Remaining features are: ', current_features)
            current_features, dropped_feature, score, score_std =\
                self._find_next_feature_to_drop(X, y, current_features, removed_features)
            
            if self.verbose >= 1:
                print('\nDropped {}. Score is: {:.4f}\n'.format(dropped_feature,
                                                              score
                                                             ))

            n_features -= 1
            scores[n_features] = {'dropped': dropped_feature,
                                  'scores_mean': score,
                                  'scores_std': score_std
                                 }
            removed_features.append(dropped_feature)

        return scores



class RemoveFeatures(BaseEstimator, TransformerMixin):
    
    def __init__(self, feats_to_remove):
        
        self.feats_to_remove = feats_to_remove
        
        
    def fit(self, X = None, y = None):
        return self
    
    def transform(self, X, y = None):
        
        X_after_drop = X.copy()
        for feature in self.feats_to_remove:
            try:
                X_after_drop.drop(columns = [feature], inplace = True)
            except KeyError:
                pass
        
        return X_after_drop