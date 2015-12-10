
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

    # read in lines data and limit to current season
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')

    return df_lines, df_bs, df_teams

def write_dataframes(df_bs, df_teams, year):
    year_indicator = "%02d" % (year,)
    players_filepath = './data/bs' + year_indicator + '.h5'
    team_filepath = './data/tbs' + year_indicator + '.h5'

    df_bs.to_hdf(players_filepath, 'df_bs', format='table', mode='w',
                 complevel=6, complib='blosc')
    df_teams.to_hdf(team_filepath, 'df_team_bs', format='table',
                    mode='w', complevel=6, complib='blosc')


def move_last_column_to_first(df):
    cols = df.columns.values.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    return df


def add_date_to_df(df_lines, df):
    df_dates = df_lines['Date']
    df = df.join(df_dates, on='GAME_ID')
    return df


if __name__ == "__main__":
    year = 3

    for year in range(3, 15):
        df_lines, df_bs, df_teams = load_dataframes(year)

        print 'df_bs shape: '
        print df_bs.shape
        print 'df_teams shape: '
        print df_teams.shape

        df_bs = add_date_to_df(df_lines, df_bs)
        df_teams = add_date_to_df(df_lines, df_teams)
        df_bs = move_last_column_to_first(df_bs)
        df_teams = move_last_column_to_first(df_teams)

        print 'new df_bs shape: '
        print df_bs.shape
        print 'new df_teams shape: '
        print df_teams.shape

        print df_teams.tail()

        write_dataframes(df_bs, df_teams, year)
        print year
