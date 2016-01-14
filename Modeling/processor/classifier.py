import pandas as pd
import numpy as np
import csv
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.feature_selection import SelectKBest, f_classif, RFECV
from sklearn.pipeline import Pipeline, FeatureUnion, make_pipeline, make_union
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
from sklearn.decomposition import PCA
from sklearn.linear_model import RandomizedLasso, RandomizedLogisticRegression
from scipy.stats import randint
import xgboost as xgb
import time
import timeit
import sys
from operator import itemgetter


# Utility function to report best scores
def report(grid_scores, n_top=3):
    top_scores = sorted(grid_scores, key=itemgetter(1), reverse=True)[:n_top]
    for i, score in enumerate(top_scores):
        print str(i) + '. ',
        print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
              score.mean_validation_score,
              np.std(score.cv_validation_scores)))
        print("Parameters: {0}".format(score.parameters))


def load_data(filepath):
    # load data
    df_data = pd.read_pickle('./output/' + filepath)
    # drop row with NA value (not sure why it exists)
    df_data = df_data.dropna()

    game_ids = df_data['game_id'].values
    # separate final outcome
    y = df_data['y'].values

    # drop unwanted columns and select feature set
    df_x = df_data.drop(['game_id', 'y', 'push', 'home_id', 'away_id', 'over'], axis=1)

    # colnames = df_x.columns.values
    x = df_x.values

    return df_x, x, y, game_ids


def fit_trees_model(filepath, clf):

    df_x, x, y, game_ids = load_data(filepath)

    selection = SelectKBest(f_classif, k=80)

    # clf = ExtraTreesClassifier(n_estimators=1500, n_jobs=-1, min_samples_split=6, min_samples_leaf=3, random_state=2222)

    pipeline = make_pipeline(selection, clf)

    kf = KFold(len(df_x.index), n_folds=5, shuffle=True, random_state=12323)

    scores = cross_val_score(pipeline, x, y, cv=kf)
    print filepath
    print scores
    print scores.mean()
    # grid_search = GridSearchCV(pipeline, param_grid, cv=kf, verbose=1)
    # grid_search = grid_search.fit(x, y)
    # print filepath
    # report(grid_search.grid_scores_, 50)
#

def fit_xgb_model(filepath, clf):
    # load data
    df_data = pd.read_pickle('./output/' + filepath)
    # drop row with NA value (not sure why it exists)
    df_data = df_data.dropna()

    # separate final outcome
    y = df_data['y'].values

    # drop unwanted columns and select feature set
    df_x = df_data.drop(['game_id', 'y', 'push', 'home_id', 'away_id', 'over'], axis=1)

    # colnames = df_x.columns.values
    x = df_x.values

    # dtrain = xgb.DMatrix(x, label=y)
    # param = {'max_depth': 6, 'eta': 0.005, 'silent': 1, 'objective': 'binary:logistic', 'colsample_bytree': 0.3, 'min_child_weight': 37, 'seed': 2287, 'gamma': 4}
    # num_round = 250
    kf = KFold(len(df_x.index), n_folds=5, shuffle=True, random_state=12323)

    print 'running cross validation: ',
    print filepath

    scores = cross_val_score(clf, x, y, cv=kf)
    print scores
    print scores.mean()
    # do cross validation, this will print result out as
    # [iteration]  metric_name:mean_value+std_value
    # std_value is standard deviation of the metric
    # cvresults = xgb.cv(param, dtrain, num_round, nfold=5, seed=1233, show_progress=False)

    # print cvresults.tail(10)


if __name__ == "__main__":
    # param_grid = {'xgbclassifier__colsample_bytree': [0.1, 0.15, 0.2],
                #   'xgbclassifier__max_depth': [4, 6, 8],
                #   'xgbclassifier__subsample': [0.5, 0.9, 1.0],
                #   'selectkbest__k': ['all']}
    xgb_clf = xgb.XGBClassifier(n_estimators=250, colsample_bytree=.3, min_child_weight=37, seed=233, learning_rate=0.005, gamma=4, max_depth=6)

    clf = ExtraTreesClassifier(n_estimators=1500, n_jobs=-1, min_samples_split=6, min_samples_leaf=3, random_state=3333)

    for year in range(2000, 2008):
        output_name = 'output_start' + str(year) + '.pkl'
        # fit_trees_model(output_name, clf)
        fit_xgb_model(output_name, xgb_clf)
    # fit_xgb_model('output_7.pkl')
