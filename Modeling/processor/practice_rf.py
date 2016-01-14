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


class ExtraTreesClassifierWithCoef(ExtraTreesClassifier):
    def fit(self, *args, **kwargs):
        super(ExtraTreesClassifierWithCoef, self).fit(*args, **kwargs)
        self.coef_ = self.feature_importances_


# Utility function to report best scores
def report(grid_scores, n_top=3):
    top_scores = sorted(grid_scores, key=itemgetter(1), reverse=True)[:n_top]
    for i, score in enumerate(top_scores):
        print str(i) + '. ',
        print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
              score.mean_validation_score,
              np.std(score.cv_validation_scores)))
        print("Parameters: {0}".format(score.parameters))


if __name__ == "__main__":
    start_time = time.time()  # timer function

    for filename in ['./output/output_10', './output/output_15']:
        # load data
        df_data = pd.read_pickle(filename)
        # drop row with NA value (not sure why it exists)
        df_data = df_data.dropna()

        df_data.head(100).to_csv('sample.csv')

        # grab game info data
        df_game_info = df_data[['game_id', 'y', 'push', 'LINE', 'home_id', 'away_id', 'over', 'total']]

        # separate final outcome
        y = df_data['y'].values
        ids = df_data['game_id'].values
        pushes = df_data['push'].values

        # drop unwanted columns and select feature set
        df_x = df_data.drop(['game_id', 'y', 'push', 'home_id', 'away_id', 'over'], axis=1)

        colnames = df_x.columns.values
        x = df_x.values

        pca = PCA(n_components=3)
        print pca.fit(x).explained_variance_ratio_
        selection = SelectKBest(f_classif, k=15)

        sel = selection.fit(x, y)
        print colnames[sel.get_support()]

        # sel = selection.fit(x, y)
        # print colnames[sel.get_support()]

        # x = sel.transform(x)
        # print colnames[x.get_support()]

        clf = ExtraTreesClassifier(n_estimators=1500, n_jobs=-1, min_samples_split=6, min_samples_leaf=3, random_state=3333)

        combined_features = FeatureUnion([('pca', pca), ('select', selection)])
        pipeline = make_pipeline(selection, clf)
        # x_features = selection.fit(x, y)
        # colnames = colnames[x_features.get_support()]

        param_grid = {'selectkbest__k': [100, 150]}

        kf = KFold(len(df_data.index), n_folds=5, shuffle=True, random_state=12232)

        grid_search = GridSearchCV(pipeline, param_grid, cv=kf, verbose=10)
        grid_search = grid_search.fit(x, y)
        print filename
        report(grid_search.grid_scores_, 10)

    sys.exit()
    scores = cross_val_score(clf, x, y, cv=kf, verbose=10)
    print scores
    print scores.mean()
    # rfecv = RFECV(estimator=clf, step=0.1, cv=kf, scoring='accuracy', verbose=5)
    # selector = rfecv.fit(x, y)
    # print 'Best: ' + str(selector.n_features_)
    # print 'Scores: ',
    # # print colnames[selector.support_]

    # param_grid = {'min_samples_split': [1, 2, 6], 'min_samples_leaf': [1, 3]}
    # param_grid = {'featureunion__pca__n_components': [0, 3, 6]}
    # pipeline = make_pipeline(combined_features, clf)
    # print pipeline
    # pipe = pipeline.fit(x, y)
    # importances = pipe.named_steps['extratreesclassifier'].feature_importances_
    # indices = np.argsort(importances)[::-1]

    # Print the feature ranking
    # print("Feature ranking:")

    # grid_search = GridSearchCV(pipeline, param_grid, cv=kf, verbose=10)
    # grid_search = grid_search.fit(x, y)
    # report(grid_search.grid_scores_, 10)
    # sys.exit()
    # scores = cross_val_score(pipeline, x, y, cv=kf, verbose=10)
    # print scores
    # print scores.mean()
    # sys.exit()

    # output file
    f = open('predictions.csv', 'wb')
    f_csv = csv.writer(f)
    f_csv.writerow(['game_id', 'pred', 'actual', 'push', 'wins'])

    winrate = 0.0
    for train_index, test_index in kf:
        print 'Training...'
        x_train = x[train_index]
        y_train = y[train_index]

        features = combined_features.fit(x_train, y_train)
        x_train = features.transform(x_train)
        pipe = clf.fit(x_train, y_train)

        print 'Testing...'
        x_test = x[test_index]
        y_test = y[test_index]
        x_test = features.transform(x_test)
        ids_test = ids[test_index]
        pushes_test = pushes[test_index]

        output = pipe.predict(x_test).astype(int)

        print 'Test accuracy: '
        print pipe.score(x_test, y_test)

        print 'Writing...'
        preds = pipe.predict_proba(x_test)
        wins = (output == y_test).astype(int)
        f_csv.writerows(zip(ids_test, output, y_test, pushes_test, wins, preds[:, 0], preds[:, 1]))
        winrate = winrate + float(pipe.score(x_test, y_test))

    f.close()
    print 'Overall score: ',
    print winrate / len(kf)
    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
