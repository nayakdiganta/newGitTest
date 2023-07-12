# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:56:10 2022

@author: s256351
"""


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyodbc
import os
import joblib
from sklearn.preprocessing import LabelBinarizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import roc_curve, precision_recall_curve, auc, make_scorer, recall_score, accuracy_score, precision_score, confusion_matrix
from sklearn.metrics import f1_score,accuracy_score
from sklearn.metrics import plot_confusion_matrix
import matplotlib.pyplot as plt
plt.style.use("ggplot")


# read training data

data_dir = 'H:\\dsmpa\\Shane\\Analytics\\meter_ties\\updated\\'
os.chdir(data_dir)
alls = pd.read_pickle('training_data_RF_11_22.pkl')


#### Break into test and training data 


X = alls.drop(['meter_id','issue','version'], axis = 1)
y = alls['issue']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30,random_state=58,stratify=y)

#### Use Hyperparameter Tuning to find the best model


clf = RandomForestClassifier(n_jobs=-1)

param_grid = {
    'min_samples_split': [3, 5, 10], 
    'n_estimators' : [100, 300],
    'max_depth': [3, 5, 15, 25],
    'max_features': [1,2,3,4,5]
}

scorers = {
    'precision_score': make_scorer(precision_score),
    'recall_score': make_scorer(recall_score),
    'accuracy_score': make_scorer(accuracy_score)
}


def grid_search_wrapper(refit_score='precision_score'):
    """
    fits a GridSearchCV classifier using refit_score for optimization
    prints classifier performance metrics
    """
    skf = StratifiedKFold(n_splits=10)
    grid_search = GridSearchCV(clf, param_grid, scoring=scorers, refit=refit_score,
                           cv=skf, return_train_score=True, n_jobs=-1, error_score = 'raise')
    grid_search.fit(X_train.values, y_train.values)

    # make the predictions
    y_pred = grid_search.predict(X_test.values)

    print('Best params for {}'.format(refit_score))
    print(grid_search.best_params_)

    # confusion matrix on the test data.
    print('\nConfusion matrix of Random Forest optimized for {} on the test data:'.format(refit_score))
    print(pd.DataFrame(confusion_matrix(y_test, y_pred),
                 columns=['pred_neg', 'pred_pos'], index=['neg', 'pos']))
    return grid_search

grid_search_clf = grid_search_wrapper(refit_score='accuracy_score')