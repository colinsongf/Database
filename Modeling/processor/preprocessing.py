import pandas as pd
import tables
import numpy as np
import cPickle
import json
import copy
import csv

# username for dropbox folder
username = 'RL'

# filepath for dropbox folder
filepath = '/Users/' + username + '/Dropbox/Public/NBA_data/'

# choose the years to run the script for, min_year of 3 starts from 2003
min_year, max_year = 0, 14

# columns that should be string value
str_cols = ['TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_NAME',
            'START_POSITION', 'COMMENT']
str_team_cols = ['TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY']
str_shot_cols = ['GRID_TYPE', 'PLAYER_NAME', 'TEAM_NAME', 'EVENT_TYPE',
                 'ACTION_TYPE', 'SHOT_TYPE', 'SHOT_ZONE_BASIC',
                 'SHOT_ZONE_AREA', 'SHOT_ZONE_RANGE', 'SHOT_DISTANCE']


def load_shots_dataframes():
    folder_filepath = '/Users/RL/Dropbox/Public/NBA_data/xefg/'
    global_filepath = folder_filepath + 'global_xefg_by_date.csv'
    df_xefg = pd.read_csv(global_filepath, index_col=['player_size', 'Date'])

    team_filepath = folder_filepath + 'team_xefg.csv'
    df_team_xefg = pd.read_csv(team_filepath, index_col=['season', 'team_id', 'oord', 'size', 'Date'])

    player_filepath = folder_filepath + 'player_xefg.csv'
    df_player_xefg = pd.read_csv(player_filepath, index_col=['player_id', 'date'])

    return df_xefg, df_team_xefg, df_player_xefg


def create_xefg():
    df_global_xefg, df_team_xefg, df_player_xefg = load_shots_dataframes()

    df_player_xefg = df_player_xefg.sort_index().drop('player_name', axis=1)
    df_team_xefg = df_team_xefg.sort_index()
    df_global_xefg = df_global_xefg.sort_index()

    write_dataframe(df_player_xefg, 'player_xefg')
    write_dataframe(df_team_xefg, 'team_xefg')
    write_dataframe(df_global_xefg, 'global_xefg')


def write_dataframe(df, name):
    filepath = './data/temp/' + name + '.h5'
    table_name = 'df_' + name
    df.to_hdf(filepath, table_name, format='table', mode='w', complevel=6, complib='blosc')


# read in the csv with line data
def load_lines():
    line_filepath = filepath + 'NBA OU data (since 2000).csv'
    df_lines = pd.read_csv(line_filepath, index_col='game_id')
    return df_lines


def add_date_to_df(df_lines, df):
    '''
    add_date_to_df: Takes in df of line data (from OU file) and another dataframe. Adds in the date column from the line data at the end of the given dataframe.
    '''
    df_dates = df_lines['Date']
    df = df.join(df_dates, on='GAME_ID')
    return df


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
def create_bs(df_lines):
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
        season_cond = '20' + year_indicator
        output_name = 'bs' + year_indicator + '.h5'
        team_output_name = 't' + output_name

        # select lines data for current year
        df_lines_curr = df_lines[df_lines['Season'] == int(season_cond)]

        # add date column to dataframe
        df_bs, df_team_bs = add_date_to_df(df_lines, df_bs), add_date_to_df(df_lines_curr, df_team_bs)
        df_bs, df_team_bs = move_last_column_to_first(df_bs), move_last_column_to_first(df_team_bs)
        print 'Added date column'

        # add opp id column to dataframe
        df_bs, df_team_bs = set_opp_id(df_bs), set_opp_id(df_team_bs)
        print 'Added opponent ID column'

        for df in [df_bs, df_team_bs]:
            df['GAME_ID'] = df['GAME_ID'].astype(int)
            df['TEAM_ID'] = df['TEAM_ID'].astype(int)
            df['OPP_ID'] = df['OPP_ID'].astype(int)

        df_bs['PLAYER_ID'] = df_bs['PLAYER_ID'].astype(int)

        # fill in missing date values
        df_dates = pd.read_csv('./preprocessing/missing_dates.csv', index_col='GAME_ID')
        df_team_bs = df_team_bs.set_index('GAME_ID')
        df_bs = df_bs.set_index('GAME_ID')
        game_ids = pd.unique(df_dates.index.values)

        for game_id in game_ids:
            if game_id in df_team_bs.index.values:
                df_team_bs.loc[game_id, 'Date'] = df_dates.at[game_id, 'Date']
            if game_id in df_bs.index.values:
                df_bs.loc[game_id, 'Date'] = df_dates.at[game_id, 'Date']

        df_team_bs = df_team_bs.reset_index()
        df_bs = df_bs.reset_index()

        # convert minutes column to float datatype
        df_bs['MIN'] = df_bs['MIN'].apply(lambda x: x / np.timedelta64(1, 'm'))
        df_team_bs['MIN'] = df_team_bs['MIN'].apply(lambda x: x / np.timedelta64(1, 'm'))

        # convert data columns in team_bs to float type
        df_team_bs.loc[:, 'FGM':] = df_team_bs.loc[:, 'FGM':].astype(np.float64)

        # add player positions column
        df_positions = pd.read_csv('./preprocessing/player_positions.csv', index_col='PLAYER_ID')
        df_positions = df_positions[df_positions['Season'] == int(season_cond)]

        df_positions = df_positions.drop(['Season'], axis=1)
        df_bs = df_bs.drop(['START_POSITION', 'TEAM_CITY'], axis=1)
        df_bs = df_bs.join(df_positions, on='PLAYER_ID')
        df_bs['START_POSITION'] = df_bs['START_POSITION'].astype(np.string_)

        df_team_bs = df_team_bs.drop(['TEAM_NAME', 'TEAM_CITY'], axis=1)
        print df_bs['START_POSITION'].isnull().sum()
        print df_bs['Date'].isnull().sum()
        print df_bs.shape
        # write to hdf file for storage
        df_bs.to_hdf('./data/temp/' + output_name, 'df_bs', format='table', mode='w',
                     complevel=6, complib='blosc')
        df_team_bs.to_hdf('./data/temp/' + team_output_name, 'df_team_bs', format='table',
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


def create_shots(df_lines):
    for c in range(min_year, max_year + 1):  # iterate through seasons

        # initialize dataframe for shots data
        df_shots = init_shots(c)
        year_indicator = "%02d" % (c,)
        season_cond = '20' + year_indicator

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

        # select lines data for current year
        df_lines = df_lines[df_lines['Season'] == int(season_cond)]

        # add date column to dataframe
        df_shots = add_date_to_df(df_lines, df_shots)
        df_shots = move_last_column_to_first(df_shots)
        print 'Added date column'

        # add opp id column to dataframe
        df_shots = set_opp_id(df_shots)
        print 'Added opponent id column'

        # naming output file
        output_name = 'shots' + year_indicator + '.h5'

        # export to hdf format with compression level
        df_shots.to_hdf(output_name, 'df_shots',
                        format='table', mode='w', complevel=6, complib='blosc')


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


def load_df_lines():
    df_lines = load_lines()
    df_lines = df_lines.convert_objects(convert_numeric=True)
    df_lines.to_hdf('lines.h5', 'df_lines', format='table', mode='w', complevel=6, complib='blosc')
    df_lines = df_lines[~np.isnan(df_lines.index.values)]

    return df_lines

##############
###  MAIN  ###
##############

if __name__ == "__main__":
    df_lines = load_df_lines()
    create_bs(df_lines)

    # df_bs = pd.read_hdf('./data/temp/bs03.h5', 'df_bs')
    # df_bs2 = pd.read_hdf('./data/players/bs03.h5', 'df_bs')

    # print df_bs.dtypes
    # print df_bs2.dtypes

    # df_teams = pd.read_hdf('./data/temp/tbs02.h5', 'df_team_bs')

    # print df_bs['POSITION'].value_counts()

    # colnames = ['PLAYER_NAME', 'PLAYER_ID', 'POSITION']
    # df_identity = df_bs[colnames].set_index('PLAYER_ID')
    # df_identity = df_identity[df_identity['POSITION'].isnull()]
    # df_identity = df_identity.drop_duplicates()
    # for player_id in pd.unique(df_identity.index.values):
    # print str(player_id) + ': ' + df_identity.at[player_id, 'PLAYER_NAME']
    # print df_identity
    # print df_identity

    # create_shots(df_lines)
    # df_shots = pd.read_hdf('shots01.h5', 'df_shots')
    # print df_shots.head()
    # df_bs, df_team_bs = initialize_bs(3)
    # print df_bs.dtype
    # create_bs()
    # create_bs()
    # create_shots()  # run the script to create the shots data
    # df_bs = pd.read_hdf('bs03.h5')
    # df_shots = pd.read_hdf('shots03.h5', 'df_shots')  # reads hdf file into pandas df
    # print df_shots.head()  # prints first 5 rows of the dataframe
    #
    # print df_shots.tail()
    # df_lines = get_lines()
    # df_lines = df_lines.convert_objects(convert_numeric=True)
    # print df_lines.head()
    # df_lines.to_hdf('lines.h5', 'df_lines', format='table', mode='w', complevel=6, complib='blosc')
    # create_bs()
