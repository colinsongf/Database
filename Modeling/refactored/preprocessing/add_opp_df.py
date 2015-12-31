
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


def write_dataframes(df_bs, df_teams, df_shots, year):
    '''
    write_dataframes: writes dataframes to hdf files
    '''
    year_indicator = "%02d" % (year,)
    players_filepath = './data/temp/bs' + year_indicator + '.h5'
    team_filepath = './data/temp/tbs' + year_indicator + '.h5'
    shots_filepath = './data/temp/shots' + year_indicator + '.h5'

    df_bs.to_hdf(players_filepath, 'df_bs', format='table', mode='w',
                 complevel=6, complib='blosc')
    print 'bs'
    df_teams.to_hdf(team_filepath, 'df_team_bs', format='table',
                    mode='w', complevel=6, complib='blosc')
    print 'teams'
    df_shots.to_hdf(shots_filepath, 'df_shots', format='table',
                    mode='w', complevel=6, complib='blosc')
    print 'shots'


def set_opp_id(df):
    dict_ids = create_dict_of_team_ids(df)
    df['OPP_ID'] = df.apply(lambda row: determine_opp_id(df, row, dict_ids), axis=1)
    df = put_opp_next_to_team(df)
    return df


def put_opp_next_to_team(df):
    colnames = df.columns.values.tolist()
    colnames.remove('OPP_ID')
    opp_id_index = colnames.index('TEAM_ID') + 1
    colnames.insert(opp_id_index, 'OPP_ID')
    return df[colnames]


def determine_opp_id(df, row, dict_ids):
    game_id = row['GAME_ID']
    curr_team_id = row['TEAM_ID']

    both_teams = dict_ids[game_id]

    if both_teams.size != 2:
        print "ERROR"

    opp_id = both_teams[0] if (both_teams[0] != curr_team_id) else both_teams[1]

    return opp_id


def create_dict_of_team_ids(df):
    both_teams = {}
    for game_id in pd.unique(df['GAME_ID'].values.tolist()):
        df_curr = df[df['GAME_ID'] == game_id]
        curr_teams = pd.unique(df_curr['TEAM_ID'].values)

        if curr_teams.size != 2:
            print "ERROR"
        both_teams[game_id] = curr_teams

    return both_teams

if __name__ == "__main__":
    year = 3
    for year in range(3, 15):
        df_lines, df_bs, df_teams, df_shots = load_dataframes(year)

        df_bs, df_teams, df_shots = set_opp_id(df_bs), set_opp_id(df_teams), set_opp_id(df_shots)

        # write dataframes to hdf files
        write_dataframes(df_bs, df_teams, df_shots, year)
        print year
