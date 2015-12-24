
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


def set_globals():
    global username, filepath, min_year, max_year, str_cols, str_shot_cols, str_team_cols

    # username for dropbox folder
    username = 'RL'

    # filepath for dropbox folder
    filepath = '/Users/' + username + '/Dropbox/Public/NBA_data/'

    # choose the years to run the script for, min_year of 3 starts from 2003
    min_year, max_year = 3, 14

    # columns that should be string value
    str_cols = ['TEAM_ABBREVIATION', 'TEAM_CITY', 'TEAM_NAME', 'FIRST_NAME', 'LAST_NAME']
    str_team_cols = ['TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY']
    str_shot_cols = ['GRID_TYPE', 'PLAYER_NAME', 'TEAM_NAME', 'EVENT_TYPE',
                     'ACTION_TYPE', 'SHOT_TYPE', 'SHOT_ZONE_BASIC',
                     'SHOT_ZONE_AREA', 'SHOT_ZONE_RANGE', 'SHOT_DISTANCE']


def read_and_convert_bs(df_init):

    # selects out the part of the json file with team and player boxscores
    df_inactives = pd.DataFrame(data=df_init.iloc[9, 2],
                                columns=df_init.iloc[9, 0])

    # converts other numbers to float or int format
    df_inactives = df_inactives.convert_objects(convert_numeric=True)

    # converts string columns to compatible string type
    df_inactives[str_cols] = df_inactives[str_cols].astype(np.string_)

    return df_inactives


# creates a new dataframe using the first game of the season
def initialize_inactives(c):
    m = 1

    # formatted strings for year num and game num
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'

    # reads in json file into a pandas dataframe
    df_init = pd.read_json(filepath + suffix)

    # converts to proper datatypes
    df_inactives = read_and_convert_bs(df_init)

    # add game id column
    game_id = str(2) + year_indicator + game_indicator
    df_inactives['GAME_ID'] = int(game_id)

    return df_inactives


# adds a game to existing pandas dataframe
def add_game_inactives(c, m):

    # formatted strings
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)

    # attempt to open a json file, return error if this game doesn't exist
    try:
        suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'
        f = open(filepath + suffix, 'r')
    except:
        return -1

    # read into a dataframe then close file
    df_init = pd.read_json(f)
    f.close()

    # convert types
    df_inactives = read_and_convert_bs(df_init)

    # add game id column
    game_id = str(2) + year_indicator + game_indicator
    df_inactives['GAME_ID'] = int(game_id)

    return df_inactives


# forms a full pandas dataframe containing boxscore data
def create_inactives(df_lines):
    for c in range(min_year, max_year + 1):  # iterate through seasons of data

        # creating strings for output file naming
        year_indicator = "%02d" % (c,)
        season_cond = '20' + year_indicator
        output_name = 'inactives' + year_indicator + '.h5'

        # initialize pandas dataframe for team and player boxscores
        df_inactives = initialize_inactives(c)

        for m in range(2, 1400):  # iterate through games of data
            df_add = add_game_inactives(c, m)  # create temp frame with new game

            if type(df_add) is int:  # skip if file open failed
                continue

            if len(df_add.index) == 0:  # skip if no inactives
                continue

            # concatenate to current dataframe
            df_inactives = pd.concat([df_inactives, df_add])
            print year_indicator + ': ' + str(m)

        df_inactives = add_date_to_df(df_lines, df_inactives)
        # write to hdf file for storage
        df_inactives.to_hdf(output_name, 'df_inactives', format='table', mode='w', complevel=6, complib='blosc')

        # print year in terminal for progress tracking
        print 'Year 20' + str(year_indicator) + ' complete'


def add_date_to_df(df_lines, df):
    '''
    add_date_to_df: Takes in df of line data (from OU file) and another dataframe. Adds in the date column from the line data at the end of the given dataframe.
    '''
    df_dates = df_lines['Date']
    df = df.join(df_dates, on='GAME_ID')
    return df


def load_df_lines():
    line_filepath = filepath + 'NBA OU data (since 2000).csv'
    df_lines = pd.read_csv(line_filepath, index_col='game_id')
    df_lines = df_lines.convert_objects(convert_numeric=True)
    df_lines.to_hdf('lines.h5', 'df_lines', format='table', mode='w', complevel=6, complib='blosc')
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    return df_lines


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
    inactives_filepath = 'inactives' + year_indicator + '.h5'
    # read in lines data and limit to current season
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')
    df_shots = pd.read_hdf(shots_filepath, 'df_shots')
    df_inactives = pd.read_hdf(inactives_filepath, 'df_inactives')

    return df_lines, df_bs, df_teams, df_shots, df_inactives

def write_team_dataframe(df_teams, year):
    year_indicator = "%02d" % (year,)
    team_filepath = './data/temp/tbs' + year_indicator + '.h5'

    df_teams.to_hdf(team_filepath, 'df_team_bs', format='table',
                    mode='w', complevel=6, complib='blosc')


if __name__ == "__main__":
    set_globals()
    min_year, max_year = 3, 3

    for year in xrange(min_year, max_year + 1):
        df_lines, df_bs, df_teams, df_shots, df_inactives = load_dataframes(year)

        df_bs = df_bs[~np.isnan(df_bs['Date'])]
        df_inactives = df_inactives[~np.isnan(df_inactives['Date'])]

        df_dnd = df_bs[df_bs['COMMENT'].str.contains('DND', na=False)]
        df_without_dnd = df_bs[~df_bs['COMMENT'].str.contains('DND', na=False)]
        df_nwt = df_without_dnd[df_without_dnd['COMMENT'].str.contains('NWT', na=False)]

        df_teams = df_teams.set_index(['TEAM_ID', 'Date'])
        df_teams = df_teams.sort_index()

        df_teams['PLAYER_IDS'] = np.empty((len(df_teams), 0)).tolist()
        df_teams['INACTIVE_IDS'] = np.empty((len(df_teams), 0)).tolist()
        df_teams['DND_IDS'] = np.empty((len(df_teams), 0)).tolist()

        df_bs.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'PLAYER_IDS'].append(row['PLAYER_ID'])), axis=1)
        print 'BS complete'
        # print df_dnd.iloc[23]
        # print df_teams.loc[1610612757, 39043].INACTIVE_IDS

        df_dnd.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'DND_IDS'].append(row['PLAYER_ID'])), axis=1)

        df_nwt.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'DND_IDS'].append(row['PLAYER_ID'])), axis=1)
        print 'DND NWT complete'

        df_inactives.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'INACTIVE_IDS'].append(row['PLAYER_ID'])), axis=1)
        print 'INACTIVES complete'

        df_teams.convert_objects()

        df_teams.reset_index(inplace=True)
        df_teams.dtypes
        write_team_dataframe(df_teams, year)

    # print df_teams.head()
    # df_sums.apply((lambda row: bs_avg[player_id].update({row['GAME_ID']: row.to_dict()})), axis=1)
