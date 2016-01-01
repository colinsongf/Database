import pandas as pd
import numpy as np
import csv
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import KFold
import time
import timeit


if __name__ == "__main__":
    start_time = time.time()  # timer function

    # load data
    df_data = pd.read_pickle('data.p')

    # drop row with NA value (not sure why it exists)
    df_data = df_data.dropna()

    # grab game info data
    df_game_info = df_data[['game_id', 'y', 'PUSH', 'LINE', 'home_team', 'away_team']]

    # separate final outcome
    y = df_data['y'].values

    # drop unwanted columns and select feature set
    x = df_data.drop(['game_id', 'y', 'PUSH', 'home_team', 'away_team'], axis=1)
    x = x.values

    print df_data.shape

    # select out game_ids, push info for outputting
    ids = df_game_info['game_id'].values
    pushes = df_game_info['PUSH']

    # use KFolds for CV
    kf = KFold(len(df_data.index), n_folds=10, shuffle=True)

    # output file
    f = open('predictions.csv', 'wb')
    f_csv = csv.writer(f)
    f_csv.writerow(['game_id', 'pred', 'actual', 'push', 'wins'])

    for train_index, test_index in kf:
        clf = RandomForestClassifier(n_estimators=800, min_samples_leaf=3, min_samples_split=6, n_jobs=2)
        print 'Training...'
        x_train = x[train_index]
        y_train = y[train_index]
        clf = clf.fit(x_train, y_train)

        print 'Testing...'
        x_test = x[test_index]
        y_test = y[test_index]
        ids_test = ids[test_index]
        pushes_test = pushes[test_index]

        output = clf.predict(x_test).astype(int)

        print 'Test accuracy: ',
        print clf.score(x_test, y_test)

        print 'Writing...'
        wins = (output == y_test).astype(int)
        f_csv.writerows(zip(ids_test, output, y_test, pushes_test, wins))

    f.close()
    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
