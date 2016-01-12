import pandas as pd
import numpy as np
import utilities
import processors


def dict_from_row(row, dict_with_date_keys):
    '''
    dict_from_row: Helper function to be used with df.apply() to create dicts
    with key:value pairs of dates:dict of stats on that date.
    '''
    curr_date = row.name if np.isscalar(row.name) else row.name[-1]
    curr_dict = dict(zip(row.index.values, row.values))
    dict_with_date_keys[curr_date] = curr_dict


class RollingCalculator(object):
    '''
    RollingCalculator: Calculates rolling sum/average stats
    on dataframes and converts the data into a dict.
    Uses pandas pd.rolling_window() functions to calculate stats,
    then df.apply() to convert each row into a dict

    Used by BoxscoreDataProcessor and ShotsDataProcessor classes.

    Instantiates with history_window and min_games attribute.

    Attributes:
    - steps: Number of games to calculate the rolling sum/average over
    - min_games: Minimum games to start the window from
    '''

    def __init__(self, history_window, min_games):
        self.steps = history_window
        self.min_games = min_games

    def rolling_sum_to_dict(self, df, dict_with_dates):
        '''
        rolling_sum_to_dict: Takes in a dataframe and an empty dict. Populates
        the dict with date keys and dicts of data as values.
        '''
        # create a dataframe containing rolling sum data
        df_sums = pd.rolling_sum(df, window=self.steps, min_periods=self.min_games)

        df_sums.apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)

        # return df_sums.to_dict('index')

    def rolling_avg_to_dict(self, df, dict_with_dates):
        '''
        rolling_avg_to_dict: Same as rolling_sum but with a rolling mean
        '''
        df_avg = pd.rolling_mean(df, window=self.steps, min_periods=self.min_games)

        # return df_avg.to_dict('index')
        df_avg.apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)

    def rolling_shots_to_dict(self, df, dict_with_dates, colnames):
        '''
        Takes in a dataframe of xefg/shots data as well as a list of desired
        column names to use for data. Populates the data into a dict keyed by
        dates.
        '''
        # df = df.select_dtypes(include=[float, int])
        # calculate rolling sum
        df_xefg_sum = pd.rolling_sum(df, window=self.steps, min_periods=self.min_games)

        # get list of column names corresponding to attempts by zone
        attempt_cols = [zone + '_attempt' for zone in processors.ShotsDataProcessor.shot_zones]

        # sum for total attempts
        df_xefg_sum['tot_attempt'] = df_xefg_sum[attempt_cols].sum(axis=1)

        # iterate through each shot zone
        for zone in processors.ShotsDataProcessor.shot_zones:
            # calculate pps as total points / total attempts
            df_xefg_sum[zone + '_pps'] = df_xefg_sum[zone + '_pts'].div(df_xefg_sum[zone + '_attempt'])
            # calculate frequency of each shot type
            df_xefg_sum[zone + '_freq'] = df_xefg_sum[zone + '_attempt'].div(df_xefg_sum['tot_attempt'])

        # reset PPS to 0 if attempts=0 (to adjust for division by zero)
        df_xefg_sum[(df_xefg_sum.isin({np.inf, -np.inf}))] = 0
        df_xefg_sum[np.isnan(df_xefg_sum)] = 0

        # return df_xefg_sum[colnames].to_dict('index')
        # populate player shots dict with calculated data
        df_xefg_sum[colnames].apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)


class EWRollingCalculator(RollingCalculator):
    def __init__(self, history_window, min_games, span):
        RollingCalculator.__init__(self, history_window, min_games)
        self.span = span

    def rolling_sum_to_dict(self, df, dict_with_dates):
        '''
        rolling_sum_to_dict: Takes in a dataframe and an empty dict. Populates
        the dict with date keys and dicts of data as values.
        '''

        # create a dataframe containing rolling sum data
        df_sums = pd.ewma(df, span=self.span, min_periods=self.min_games)

        game_counter = np.clip(np.arange(1, len(df_sums.index)), 1, self.history_window)
        df_sums = df_sums.multiply(game_counter, axis=0)

        df_sums.apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)

    def rolling_avg_to_dict(self, df, dict_with_dates):
        '''
        rolling_avg_to_dict: Same as rolling_sum but with a rolling mean
        '''
        df_avg = pd.ewma(df, span=self.span, min_periods=self.min_games)

        # return df_avg.to_dict('index')
        df_avg.apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)

    def rolling_shots_to_dict(self, df, dict_with_dates, colnames):
        '''
        Takes in a dataframe of xefg/shots data as well as a list of desired
        column names to use for data. Populates the data into a dict keyed by
        dates.
        '''
        # calculate rolling sum
        df_xefg_sum = pd.ewma(df, span=self.span, min_periods=self.min_games)

        # multiply weighted moving average by number of games to get approx s um
        game_counter = np.clip(np.arange(1, len(df_xefg_sum.index)), 1, self.history_window)
        df_xefg_sum = df_xefg_sum.multiply(game_counter, axis=0)

        # get list of column names corresponding to attempts by zone
        attempt_cols = [zone + '_attempt' for zone in processors.ShotsDataProcessor.shot_zones]

        # sum for total attempts
        df_xefg_sum['tot_attempt'] = df_xefg_sum[attempt_cols].sum(axis=1)

        # iterate through each shot zone
        for zone in processors.ShotsDataProcessor.shot_zones:
            # calculate pps as total points / total attempts
            df_xefg_sum[zone + '_pps'] = df_xefg_sum[zone + '_pts'].div(df_xefg_sum[zone + '_attempt'])
            # calculate frequency of each shot type
            df_xefg_sum[zone + '_freq'] = df_xefg_sum[zone + '_attempt'].div(df_xefg_sum['tot_attempt'])

        # reset PPS to 0 if attempts=0 (to adjust for division by zero)
        df_xefg_sum[(df_xefg_sum.isin({np.inf, -np.inf}))] = 0
        df_xefg_sum[np.isnan(df_xefg_sum)] = 0

        # return df_xefg_sum[colnames].to_dict('index')
        # populate player shots dict with calculated data
        df_xefg_sum[colnames].apply((lambda row: dict_from_row(row, dict_with_dates)), axis=1)
