import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.feature_selection import SelectKBest, f_classif, RFECV
from sklearn.pipeline import Pipeline, FeatureUnion, make_pipeline, make_union
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
from sklearn.decomposition import PCA
from sklearn.linear_model import RandomizedLasso, RandomizedLogisticRegression
from scipy.stats import randint
from operator import itemgetter
import matplotlib
import seaborn as sns


class SearchReporter(object):
    def __init__(self, search):
        self.search = search
        self.scores = search.grid_scores_

    def report(self, n_top=10):
        top_scores = sorted(self.grid_scores, key=itemgetter(1), reverse=True)[:n_top]
        for i, score in enumerate(top_scores):
            print str(i) + '. ',
            print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
                  score.mean_validation_score,
                  np.std(score.cv_validation_scores)))
            print("Parameters: {0}".format(score.parameters))

    def barplot(self):
        df_param_search = self._grid_to_df(self.scores)

        for param in pd.unique(df_param_search.index.values):
            sns.boxplot(x='value', y='score', data=df_param_search.loc[param])
            sns.plt.title(param)
            sns.plt.show()

    def lmplot(self):
        df_param_search = self._grid_to_df(self.scores)

        df_param_search = df_param_search.reset_index()
        sns.lmplot(x='value', y='score', data=df_param_search,
                   col='param', lowess=True)

    @staticmethod
    def _grid_to_df(scores):
        dataholder = []
        params = scores[0].parameters.keys()
        for param in params:
            for search in scores:
                dataholder.append([param, search.parameters[param],
                                   search.mean_validation_score])

        df_param_search = pd.DataFrame.from_records(dataholder, columns=['param', 'value', 'score'])
        df_param_search = df_param_search.set_index('param')

        return df_param_search
