import pandas as pd
import numpy as np
import csv
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
from sklearn.decomposition import PCA
from sklearn.linear_model import RandomizedLasso, RandomizedLogisticRegression
from scipy.stats import randint
import xgboost as xgb
import time
import timeit
import sys


if __name__ == "__main__":
    start_time = time.time()  # timer function

    # load data
    df_data = pd.read_pickle('data2.p')

    # drop row with NA value (not sure why it exists)
    df_data = df_data.dropna()

    # grab game info data
    df_game_info = df_data[['game_id', 'y', 'PUSH', 'LINE', 'home_team', 'away_team']]

    # separate final outcome
    y = df_data['y'].values
    ids = df_data['game_id'].values
    pushes = df_data['PUSH'].values

    # drop unwanted columns and select feature set
    df_x = df_data.drop(['game_id', 'y', 'PUSH', 'home_team', 'away_team'], axis=1)

    colnames = df_x.columns.values
    x = df_x.values

    pca = PCA(n_components=3)
    print pca.fit(x).explained_variance_ratio_
    selection = SelectKBest(f_classif, k=80)
    # x = selection.fit_transform(x, y)
    # print colnames[x.get_support()]
    combined_features = FeatureUnion([('pca', pca), ('select', selection)])
    clf = xgb.XGBClassifier()

    # x = combined_features.fit(x, y).transform(x)

    # pipeline = Pipeline([('features', combined_features), ('clf', clf)])

    kf = KFold(len(df_data.index), n_folds=5)

    # output file
    f = open('predictions.csv', 'wb')
    f_csv = csv.writer(f)
    f_csv.writerow(['game_id', 'pred', 'actual', 'push', 'wins'])

    winrate = 0.0
    for train_index, test_index in kf:
        print 'Training...'
        x_train = x[train_index]
        y_train = y[train_index]
        print x_train.shape
        sel = selection.fit(x_train, y_train)
        x_train = sel.transform(x_train)
        # pcafit = pca.fit(x_train, y_train)
        # x_train = pcafit.transform(x_train)

        print x_train.shape
        clf = clf.fit(x_train, y_train)
        # pipe = pipeline.fit(x[train_index], y[train_index])
        # print clf.n_features_

        print 'Testing...'
        x_test = x[test_index]
        print x_test.shape
        x_test = sel.transform(x_test)
        # x_test = combined_features.transform(x_test)
        print x_test.shape
        y_test = y[test_index]
        ids_test = ids[test_index]
        pushes_test = pushes[test_index]

        output = clf.predict(x_test).astype(int)

        print 'Test accuracy: '
        print clf.score(x_test, y_test)

        print 'Writing...'
        preds = clf.predict_proba(x_test)
        wins = (output == y_test).astype(int)
        f_csv.writerows(zip(ids_test, output, y_test, pushes_test, wins, preds[:, 0], preds[:, 1]))
        winrate = winrate + float(clf.score(x_test, y_test))

    f.close()
    print 'Overall score: ',
    print winrate / len(kf)
    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
