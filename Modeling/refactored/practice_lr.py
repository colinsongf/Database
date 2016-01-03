import pandas as pd
import numpy as np
import csv
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import KFold
from sklearn import preprocessing
from sklearn.feature_selection import SelectKBest
from sklearn.pipeline import Pipeline
import time
import timeit


if __name__ == "__main__":
    start_time = time.time()  # timer function

    # load data
    df_data = pd.read_pickle('data.p')

    df_data.to_csv('output.csv')
    # drop row with NA value (not sure why it exists)
    df_data = df_data.dropna()
    # grab game info data
    df_game_info = df_data[['game_id', 'y', 'PUSH', 'LINE', 'home_team', 'away_team']]

    # separate final outcome
    y = df_data['y'].values

    # drop unwanted columns and select feature set
    df_x = df_data.drop(['game_id', 'y', 'PUSH', 'home_team', 'away_team'], axis=1)
    x = df_x.values
    x = preprocessing.scale(x)
    # x = df_data[['LINE', 'game_id']].values
    selection = SelectKBest(k=30)

    # select out game_ids, push info for outputting
    ids = df_game_info['game_id'].values
    pushes = df_game_info['PUSH'].values
    # use KFolds for CV
    kf = KFold(len(df_data.index), n_folds=5)

    # output file
    f = open('lr_predictions.csv', 'wb')
    f_csv = csv.writer(f)
    f_csv.writerow(['game_id', 'pred', 'actual', 'push'])

    for train_index, test_index in kf:
        clf = LogisticRegression(penalty='l2', C=0.0000000000001)
        print 'Training...'
        x_train = x[train_index]
        y_train = y[train_index]
        b = selection.fit(x_train, y_train)
        print df_x.columns.values[b.get_support()]
        x_train = selection.fit_transform(x_train, y_train)
        print x_train.shape

        clf = clf.fit(x_train, y_train)

        print 'Testing...'
        x_test = x[test_index]
        x_test = selection.transform(x_test)
        print x_test.shape
        y_test = y[test_index]
        ids_test = ids[test_index]
        pushes_test = pushes[test_index]

        output = clf.predict(x_test).astype(int)

        print 'Train Accuracy: ',
        print clf.score(x_train, y_train)
        print 'Test Accuracy: ',
        print clf.score(x_test, y_test)

        wins = (output == y_test).astype(int)
        print 'Writing...'
        f_csv.writerows(zip(ids_test, output, y_test, pushes_test, wins))

    f.close()
    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
