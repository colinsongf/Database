
import scipy.stats as ss
import pandas as pd
import tables
import numpy as np
import simplejson
import cPickle
import json
import copy
import csv
import sys


def load_dataframes(year):
    '''
    load_dataframes: reads hdf storage files and returns dataframes for a season
    '''
    # forming strings to read in files
    year_indicator = "%02d" % (year,)
    season_cond = '20' + year_indicator
    lines_filepath = './data/lines/lines.h5'
    players_filepath = './data/players/bs' + year_indicator + '.h5'
    team_filepath = './data/teams/tbs' + year_indicator + '.h5'
    # shots_filepath = './data/shots/shots' + year_indicator + '.h5'
    # read in lines data and limit to current season
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')
    # df_shots = pd.read_hdf(shots_filepath, 'df_shots')

    return df_lines, df_bs, df_teams


def write_dataframes(df_bs, df_teams, year):
    '''
    write_dataframes: writes dataframes to hdf files
    '''
    year_indicator = "%02d" % (year,)
    players_filepath = './data/temp/bs' + year_indicator + '.h5'
    team_filepath = './data/temp/tbs' + year_indicator + '.h5'

    df_bs.to_hdf(players_filepath, 'df_bs', format='table', mode='w',
                 complevel=6, complib='blosc')
    df_teams.to_hdf(team_filepath, 'df_team_bs', format='table',
                    mode='w', complevel=6, complib='blosc')


def move_last_column_to_first(df):
    '''
    Takes the last column of a dataframe and replaces it to the first column
    '''

    # get column header
    cols = df.columns.values.tolist()

    # shift last element to first
    cols = cols[-1:] + cols[:-1]

    # reorder dataframe based on new list of headers
    df = df[cols]

    return df


def add_date_to_df(df_dates, df):
    '''
    add_date_to_df: Takes in df of line data (from OU file) and another dataframe. Adds in the date column from the line data at the end of the given dataframe.
    '''
    df = df.merge(df_dates, on='GAME_ID', how='left')
    return df

def write_df_shots(df_shots):
    year_indicator = "%02d" % (year,)
    shots_filepath = './data/shots' + year_indicator + '.h5'

    df_shots.to_hdf(shots_filepath, 'df_shots', format='table', mode='w',
                 complevel=6, complib='blosc')

if __name__ == "__main__":
    df_dates = pd.read_csv('missing_dates.csv', index_col='GAME_ID')

    '''
    for year in range(3, 15):
        df_lines, df_bs, df_teams = load_dataframes(year)
        df_teams = df_teams.set_index('GAME_ID')
        df_bs = df_bs.set_index('GAME_ID')
        game_ids = pd.unique(df_dates.index.values)

        for game_id in game_ids:
            if game_id in df_teams.index.values:
                df_teams.loc[game_id, 'Date'] = df_dates.at[game_id, 'Date']
            if game_id in df_bs.index.values:
                df_bs.loc[game_id, 'Date'] = df_dates.at[game_id, 'Date']

        df_teams = df_teams.reset_index()
        df_bs = df_bs.reset_index()
        write_dataframes(df_bs, df_teams, year)

    '''
    f = open('missing_dates2.csv', 'wb')
    f_csv = csv.writer(f)
    f_csv.writerow(['game_id'])
    for year in range(3, 15):  # iterate through all seasons
        df_lines, df_bs, df_teams = load_dataframes(year)
        df_missing = df_teams[df_teams.isnull().any(axis=1)]
        missing_ids = df_missing['GAME_ID'].values
        f_csv.writerows(zip(missing_ids))
        # df_shots = add_date_to_df(df_lines, df_shots)
        # df_shots = move_last_column_to_first(df_shots)
        # write_df_shots(df_shots)
        # add dates to bs and team bs and re-order columns
        # df_bs = add_date_to_df(df_lines, df_bs)
        # df_teams = add_date_to_df(df_lines, df_teams)
        # df_bs = move_last_column_to_first(df_bs)
        # df_teams = move_last_column_to_first(df_teams)

        # write dataframes to hdf files

    f.close()
