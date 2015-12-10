
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
    min_year, max_year = 3, 3

    # columns that should be string value
    str_cols = ['TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_NAME',
                'START_POSITION', 'COMMENT']
    str_team_cols = ['TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY']
    str_shot_cols = ['GRID_TYPE', 'PLAYER_NAME', 'TEAM_NAME', 'EVENT_TYPE',
                     'ACTION_TYPE', 'SHOT_TYPE', 'SHOT_ZONE_BASIC',
                     'SHOT_ZONE_AREA', 'SHOT_ZONE_RANGE', 'SHOT_DISTANCE']


# read in the csv with line data
def get_lines():
    line_filepath = filepath + 'NBA OU data (since 2000).csv'
    df_lines = pd.read_csv(line_filepath, index_col='game_id')
    return df_lines


# picks out relevant parts of boxscore and makes column datatypes
def read_and_convert_bs(df_init):

    # selects out the part of the json file with team and player boxscores
    df_bs = pd.DataFrame(data=df_init.iloc[4, 2],
                         columns=df_init.iloc[4, 0])  # player bs
    df_team_bs = pd.DataFrame(data=df_init.iloc[5, 2],
                              columns=df_init.iloc[5, 0])  # team bs

    # converts minutes to Timedelta format to allow for summation later
    df_bs['MIN'] = df_bs['MIN'].replace({None: '0:00'})
    df_bs['MIN'] = df_bs['MIN'].str.split(':').apply(lambda x: pd.Timedelta(minutes=int(x[0]), seconds=int(x[1])))

    df_team_bs['MIN'] = df_team_bs['MIN'].replace({None: '0:00'})
    df_team_bs['MIN'] = df_team_bs['MIN'].str.split(':').apply(lambda x: pd.Timedelta(minutes=int(x[0]), seconds=int(x[1])))

    # converts other numbers to float or int format
    df_team_bs = df_team_bs.convert_objects(convert_numeric=True)
    df_bs = df_bs.convert_objects(convert_numeric=True)

    # converts string columns to compatible string type
    df_team_bs[str_team_cols] = df_team_bs[str_team_cols].astype(np.string_)
    df_bs[str_cols] = df_bs[str_cols].astype(np.string_)

    return df_bs, df_team_bs


# creates a new dataframe using the first game of the season
def initialize_bs(c):
    m = 1

    # formatted strings for year num and game num
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'

    # reads in json file into a pandas dataframe
    df_init = pd.read_json(filepath + suffix)

    # converts to proper datatypes
    df_bs, df_team_bs = read_and_convert_bs(df_init)

    return df_bs, df_team_bs

# adds a game to existing pandas dataframe
def add_game_bs(c, m):

    # formatted strings
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)

    # attempt to open a json file, return error if this game doesn't exist
    try:
        suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'
        f = open(filepath + suffix, 'r')
    except:
        return -1, -1

    # read into a dataframe then close file
    df_init = pd.read_json(f)
    f.close()
    df_bs = pd.DataFrame(data=df_init.iloc[4, 2],
                         columns=df_init.iloc[4, 0])

    # convert types
    df_bs, df_team_bs = read_and_convert_bs(df_init)

    return df_bs, df_team_bs


# forms a full pandas dataframe containing boxscore data
def create_bs():
    for c in range(min_year, max_year + 1):  # iterate through seasons of data

        # initialize pandas dataframe for team and player boxscores
        df_bs, df_team_bs = initialize_bs(c)

        for m in range(2, 1400):  # iterate through games of data
            df_add, df_team_add = add_game_bs(c, m)  # create temp frame with new game

            if type(df_add) is int:  # skip if file open failed
                continue

            # concatenate to current dataframe
            df_bs = pd.concat([df_bs, df_add])
            df_team_bs = pd.concat([df_team_bs, df_team_add])

        # creating strings for output file naming
        year_indicator = "%02d" % (c,)
        output_name = 'bs' + year_indicator + '.h5'
        team_output_name = 't' + output_name

        # write to hdf file for storage
        df_bs.to_hdf(output_name, 'df_bs', format='table', mode='w',
                     complevel=6, complib='blosc')
        df_team_bs.to_hdf(team_output_name, 'df_team_bs', format='table',
                          mode='w', complevel=6, complib='blosc')

        # print year in terminal for progress tracking
        print 'Year 20' + str(year_indicator) + ' complete'


# initializes a dataframe to store shots data in
def init_shots(c):
    m = 1  # starts at game num 1 for a season

    # forming strings for input file reading
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    suffix = 'json/shots_002' + year_indicator + game_indicator + '.json'

    # open shots file
    f = open(filepath + suffix, 'r')
    json_shots = json.load(f)
    f.close()

    # initialize dataframe and load with shots data from json file
    df_shots = pd.DataFrame(columns=json_shots['headers'],
                            data=json_shots['rowSet'])

    # convert columns to proper datatypes
    df_shots = df_shots.convert_objects(convert_numeric=True)
    df_shots[str_shot_cols] = df_shots[str_shot_cols].astype(np.string_)

    return df_shots


def add_shots(c, m):

    # forming strings to read input file
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)

    # try to open game file, return error if game number doesn't exist
    try:
        suffix = 'json/shots_002' + year_indicator + game_indicator + '.json'
        f = open(filepath + suffix, 'r')
    except:
        return -1

    # load file into json format
    json_shots = json.load(f)
    f.close()

    # load the formatted json into a pandas dataframe
    df_shots = pd.DataFrame(columns=json_shots['headers'],
                            data=json_shots['rowSet'])

    # convert columns to proper datatypes
    df_shots = df_shots.convert_objects(convert_numeric=True)
    df_shots[str_shot_cols] = df_shots[str_shot_cols].astype(np.string_)

    return df_shots


def create_shots():
    for c in range(min_year, max_year + 1):  # iterate through seasons

        # initialize dataframe for shots data
        df_shots = init_shots(c)
        year_indicator = "%02d" % (c,)

        for m in range(2, 1400):  # iterate through games within season

            # create temp dataframe with next game of data
            df_add = add_shots(c, m)

            # skip game if file open failed
            if type(df_add) is int:
                continue

            # concatenate the current game of data to the final output
            df_shots = pd.concat([df_shots, df_add])

            # print year and game number for progress tracking
            print year_indicator + ': ' + str(m)

        # naming output file
        output_name = 'shots' + year_indicator + '.h5'

        # export to hdf format with compression level
        df_shots.to_hdf(output_name, 'df_shots',
                        format='table', mode='w', complevel=6, complib='blosc')

##############
###  MAIN  ###
##############

if __name__ == "__main__":
    set_globals()
    # df_bs, df_team_bs = initialize_bs(3)
    # print df_bs.dtype
    # create_bs()
    create_bs()
    # create_shots()  # run the script to create the shots data
    # df_shots = pd.read_hdf('shots03.h5', 'df_shots')  # reads hdf file into pandas df
    # print df_shots.head()  # prints first 5 rows of the dataframe



        # create_shots()
    #
    # print df_shots.tail()
    # df_lines = get_lines()
    # df_lines = df_lines.convert_objects(convert_numeric=True)
    # print df_lines.head()
    # df_lines.to_hdf('lines.h5', 'df_lines', format='table', mode='w', complevel=6, complib='blosc')
    # create_bs()
