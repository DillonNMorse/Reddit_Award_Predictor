# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 16:01:26 2021

@author: Dillo
"""

import nltk
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy.sparse import csr_matrix
import spacy

from sklearn.base import BaseEstimator, TransformerMixin



class My_tfidf(BaseEstimator, TransformerMixin):
    
    def __init__(self, target_column_name, tfidf_num_feats = 1000, 
                 num_pca_vars = 0, drop_target = True, preprocess = True,
                 ngram_range = (1,1), **kwargs
                 ):
        
        self.target_column_name = target_column_name
        self.tfidf_num_feats = tfidf_num_feats
        self.num_pca_vars = num_pca_vars
        self.drop_target = drop_target
        self.preprocess = preprocess
        self.ngram_range = ngram_range
        self.kwargs = kwargs
        
    
        
    def _preprocess_text(self, sentence, stopwords, stemmer, tokenizer):
# =============================================================================
#         stopwords = nltk.corpus.stopwords.words('english')
#         stemmer = nltk.stem.PorterStemmer()
#         tokenizer = nltk.RegexpTokenizer(r"\w+")
# =============================================================================
        
        all_words = tokenizer.tokenize(sentence.lower())
        output_words = [stemmer.stem(word) for word in all_words if word not in stopwords]
        output_sentence = ''
        for word in output_words:
            output_sentence += word + ' '
        output_sentence.rstrip()
        return output_sentence       
        
        
    def fit(self, X, y = None):
        
        # Pass through without doing anything if no tfidf_feats
        if not self.tfidf_num_feats == 0:
            
            # Do tfidf
            self.vectorizer = TfidfVectorizer(strip_accents = 'unicode',
                                              stop_words = 'english',
                                              max_features = self.tfidf_num_feats,
                                              ngram_range = self.ngram_range
                                              )
            
            if self.preprocess == True:
                # Do some text pre-processing
                stopwords = nltk.corpus.stopwords.words('english')
                stemmer = nltk.stem.PorterStemmer()
                tokenizer = nltk.RegexpTokenizer(r"\w+")
                
                docs = X[self.target_column_name].apply(lambda x: 
                          self._preprocess_text(x, stopwords, stemmer, tokenizer))
            else:
                docs =  X[self.target_column_name]   
            
            _ = self.vectorizer.fit(docs)
            
            self.tfidf_features = self.vectorizer.get_feature_names()
            
            if not self.num_pca_vars == 0:
                # Do PCA - will need to transform training set w/ tfidf to fit PCA
                self.normalize = StandardScaler()
                self.dim_reduce = PCA(svd_solver = 'randomized',
                                      n_components = self.num_pca_vars,
                                      )
                vectors = csr_matrix(self.vectorizer.transform(docs)).todense()
                vectors = self.normalize.fit_transform(vectors)
                _ = self.dim_reduce.fit(vectors)
        
        
        return self
    
    
    
    
    
    def transform(self, X, y = None):
        
        # Pass through without doing anything if no tfidf_feats
        if not self.tfidf_num_feats == 0:        
            # Do some text pre-processing
            stopwords = nltk.corpus.stopwords.words('english')
            stemmer = nltk.stem.PorterStemmer()
            tokenizer = nltk.RegexpTokenizer(r"\w+")
            
            docs = X[self.target_column_name].apply(lambda x: 
                      self._preprocess_text(x, stopwords, stemmer, tokenizer))
            
            # Perform tfidf calculation
            vectors = self.vectorizer.transform(docs)
            
            # Perform PCA on tfidf if num_pca_vars > 0
            if self.num_pca_vars > 0:
                vectors = csr_matrix(vectors).todense()
                normalized_vectors = self.normalize.transform(vectors)
                new_vectors = self.dim_reduce.transform(normalized_vectors)
                column_names = ['tfidf_pca_{}'.format(x) 
                                for x in range(self.num_pca_vars)
                                ]
                new_df = pd.DataFrame(new_vectors,
                                      columns = column_names,
                                      index = X.index
                                      )
            # If num_pca_vars = 0 just load tfidf vectors in to a dataframe
            elif (self.num_pca_vars == 0):
                column_names =  ['tfidf_{}'.format(x) 
                                for x in range(self.tfidf_num_feats)
                                ]
                new_df = pd.DataFrame.sparse.from_spmatrix(vectors,
                                                           columns = column_names,
                                                           index = X.index,
                                                           )
            # Drop the old target column and append new tfidf/pca columns to X
            new_X = pd.concat([X, new_df], axis = 1)
            if self.drop_target:
                new_X.drop(columns = [self.target_column_name], inplace = True)
       
        elif self.tfidf_num_feats == 0:
            new_X = X.copy()
            if self.drop_target:
                new_X.drop(columns = [self.target_column_name], inplace = True)           
            
        return new_X
    
    
    
    
class MyWord2Vec(BaseEstimator, TransformerMixin):
    
    def __init__(self, title_vec_file_fname, num_pca_vars = 1):
        
        self.title_vec_file_fname = title_vec_file_fname
        self.num_pca_vars = num_pca_vars




    def fit(self, X, y = None):
        

        
        if (self.num_pca_vars > 0) & (self.num_pca_vars < 1):
            self.title_vecs = pd.read_pickle(self.title_vec_file_fname)
            X_with_w2v = X.copy().merge(self.title_vecs, on = 'id', how = 'inner')
            self.dim_reduce = PCA(n_components = self.num_pca_vars)
            self.dim_reduce.fit(X_with_w2v.iloc[:,-300:])
        elif self.num_pca_vars == 1:
            self.title_vecs = pd.read_pickle(self.title_vec_file_fname)
        elif self.num_pca_vars == 0:
            pass
        else:
            raise Exception("""num_pca_vars must be between 0 and 1, set to 0\
                            to include no w2v and set to 1 to do no pca"""
                            )
        
        return self




    def transform(self, X, y = None):
        
        if (self.num_pca_vars > 0) & (self.num_pca_vars < 1):
            X_with_w2v_temp = X.copy().merge(self.title_vecs, on = 'id', how = 'inner')
            reduced_vectors = self.dim_reduce.transform(X_with_w2v_temp.iloc[:,-300:])
            reduced_vectors_df = pd.DataFrame(reduced_vectors, index = X.index)
            X_with_w2v = pd.concat([X, reduced_vectors_df], axis = 1, join = 'inner')
        elif self.num_pca_vars == 1:
            X_with_w2v = X.copy().merge(self.title_vecs, on = 'id', how = 'inner')            
        elif self.num_pca_vars == 0:
            X_with_w2v = X.copy()

        X_with_w2v.drop(columns = ['id'], inplace = True)        

        return X_with_w2v