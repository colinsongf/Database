import pandas as pd
import numpy as np


def dict_column_sum(df_sum):
    '''
    dict_column_sum: helper function that sums the columns in a pandas dataframe and returns them to a dict

    Note: Significantly faster than pd.sum(axis=0).to_dict() for league stats, but not for player stats. Not sure why
    '''
    stats_header = df_sum.columns.values.tolist()
    dict_sum = dict(zip(stats_header, [df_sum[col].values.sum(axis=0) for col in stats_header]))
    return dict_sum


def del_keys_dict(del_keys, dict):
    '''
    del_keys_dict: delete keys in a dict. Used to drop unwanted stats.
    '''
    for key in dict.keys():
        if key in del_keys:
            del dict[key]


def count_prev_games(dates, curr_date):
    '''
    prev_date_index: Returns number of dates before
    current one in sorted dates nparray
    '''
    return np.searchsorted(dates, curr_date)


def prev_date_index(dates, curr_date):
    '''
    prev_date_index: Helper function. Searches a list (nparray) of dates for a
    given player. Returns the index along the array of the LAST date a player
    played or shot on, before the current date.
    '''
    return count_prev_games(dates, curr_date) - 1


def prev_date(dates, curr_date):
    '''
    prev_date: Returns last date within a nparray of dates.
    '''
    return dates[np.searchsorted(dates, curr_date) - 1]
