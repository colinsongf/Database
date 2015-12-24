
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
    shots_filepath = './data/shots/shots' + year_indicator + '.h5'
    # read in lines data and limit to current season
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')
    df_shots = pd.read_hdf(shots_filepath, 'df_shots')

    return df_lines, df_bs, df_teams, df_shots


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


def add_date_to_df(df_lines, df):
    '''
    add_date_to_df: Takes in df of line data (from OU file) and another dataframe. Adds in the date column from the line data at the end of the given dataframe.
    '''
    df_dates = df_lines['Date']
    df = df.join(df_dates, on='GAME_ID')
    return df


def write_df_shots(df_shots, year):
    year_indicator = "%02d" % (year,)
    shots_filepath = './data/shots' + year_indicator + '.h5'

    df_shots.to_hdf(shots_filepath, 'df_shots', format='table', mode='w',
                 complevel=6, complib='blosc')


def write_df_teams(df_teams, year):
    year_indicator = "%02d" % (year,)
    teams_filepath = './data/temp/tbs' + year_indicator + '.h5'

    df_teams.to_hdf(teams_filepath, 'df_teams', format='table', mode='w',
                 complevel=6, complib='blosc')


if __name__ == "__main__":

    for year in range(3, 15):  # iterate through all seasons
        df_lines, df_bs, df_teams, df_shots = load_dataframes(year)
        df_teams.iloc[:, 8:] = df_teams.iloc[:, 8:].astype(float)
        df_teams['MIN'] = df_teams['MIN'] / np.timedelta64(1, 'm')
        df_bs['MIN'] = df_bs['MIN'] / np.timedelta64(1, 'm')
        # write dataframes to hdf files
        write_dataframes(df_bs, df_teams, year)
        print year
