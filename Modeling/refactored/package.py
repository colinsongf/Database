
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
    username = 'RL'
    filepath = '/Users/' + username + '/Dropbox/Public/NBA_data/'
    min_year, max_year = 3, 3
    str_cols = ['TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_NAME',
                'START_POSITION', 'COMMENT']
    str_team_cols = ['TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY']
    str_shot_cols = ['GRID_TYPE', 'PLAYER_NAME', 'TEAM_NAME', 'EVENT_TYPE',
                     'ACTION_TYPE', 'SHOT_TYPE', 'SHOT_ZONE_BASIC',
                     'SHOT_ZONE_AREA', 'SHOT_ZONE_RANGE', 'SHOT_DISTANCE']

def get_lines():
    line_filepath = filepath + 'NBA OU data (since 2000).csv'
    df_lines = pd.read_csv(line_filepath, index_col='game_id')
    return df_lines


def read_and_convert_bs(df_init):
    df_bs = pd.DataFrame(data=df_init.iloc[4, 2],
                         columns=df_init.iloc[4, 0])
    df_team_bs = pd.DataFrame(data=df_init.iloc[5, 2],
                              columns=df_init.iloc[5, 0])

    df_bs['MIN'] = df_bs['MIN'].replace({None: '0:00'})
    df_bs['MIN'] = df_bs['MIN'].str.split(':').apply(lambda x: pd.Timedelta(minutes=int(x[0]), seconds=int(x[1])))

    df_team_bs['MIN'] = df_team_bs['MIN'].replace({None: '0:00'})
    df_team_bs['MIN'] = df_team_bs['MIN'].str.split(':').apply(lambda x: pd.Timedelta(minutes=int(x[0]), seconds=int(x[1])))

    df_team_bs = df_team_bs.convert_objects(convert_numeric=True)
    df_bs = df_bs.convert_objects(convert_numeric=True)

    df_team_bs[str_team_cols] = df_team_bs[str_team_cols].astype(np.string_)
    df_bs[str_cols] = df_bs[str_cols].astype(np.string_)

    # df_team_bs['MIN'] = pd.to_datetime(df_team_bs['MIN'], format='%M:%S')

    return df_bs, df_team_bs


def initialize_bs(c):
    m = 1
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'
    df_init = pd.read_json(filepath + suffix)
    df_bs, df_team_bs = read_and_convert_bs(df_init)

    return df_bs, df_team_bs



def add_game_bs(c, m):
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    try:
        suffix = 'json/bs_002' + year_indicator + game_indicator + '.json'
        f = open(filepath + suffix, 'r')
    except:
        return -1, -1
    # print f
    df_init = pd.read_json(f)
    f.close()
    df_bs = pd.DataFrame(data=df_init.iloc[4, 2],
                         columns=df_init.iloc[4, 0])

    df_bs, df_team_bs = read_and_convert_bs(df_init)

    return df_bs, df_team_bs

    # print df_bs.head()
    # print df_lines.head()


def create_bs():
    for c in range(min_year, max_year + 1):
        df_bs, df_team_bs = initialize_bs(c)
        for m in range(2, 1400):
            df_add, df_team_add = add_game_bs(c, m)
            if type(df_add) is int:
                continue
            df_bs = pd.concat([df_bs, df_add])
            df_team_bs = pd.concat([df_team_bs, df_team_add])
        year_indicator = "%02d" % (c,)
        output_name = 'bs' + year_indicator + '.h5'
        team_output_name = 't' + output_name
        df_bs.to_hdf(output_name, 'df_bs', format='table', mode='w',
                     complevel=6, complib='blosc')
        df_team_bs.to_hdf(team_output_name, 'df_team_bs', format='table',
                          mode='w', complevel=6, complib='blosc')
        print 'Year 20' + str(year_indicator) + ' complete'


def init_shots(c):
    m = 1
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    suffix = 'json/shots_002' + year_indicator + game_indicator + '.json'
    f = open(filepath + suffix, 'r')
    json_shots = json.load(f)
    f.close()
    df_shots = pd.DataFrame(columns=json_shots['headers'],
                            data=json_shots['rowSet'])
    df_shots = df_shots.convert_objects(convert_numeric=True)
    df_shots[str_shot_cols] = df_shots[str_shot_cols].astype(np.string_)
    return df_shots


def add_shots(c, m):
    year_indicator = "%02d" % (c,)
    game_indicator = "%05d" % (m,)
    try:
        suffix = 'json/shots_002' + year_indicator + game_indicator + '.json'
        f = open(filepath + suffix, 'r')
    except:
        return -1
    json_shots = json.load(f)
    f.close()
    df_shots = pd.DataFrame(columns=json_shots['headers'],
                            data=json_shots['rowSet'])
    df_shots = df_shots.convert_objects(convert_numeric=True)
    df_shots[str_shot_cols] = df_shots[str_shot_cols].astype(np.string_)

    return df_shots


def create_shots():
    for c in range(min_year, max_year + 1):
        df_shots = init_shots(c)
        year_indicator = "%02d" % (c,)

        for m in range(2, 1400):
            df_add = add_shots(c, m)
            if type(df_add) is int:
                continue
            df_shots = pd.concat([df_shots, df_add])
            print year_indicator + ': ' + str(m)
        output_name = 'shots' + year_indicator + '.h5'
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
    create_shots()
    df_shots = pd.read_hdf('shots03.h5', 'df_shots')
    print df_shots.head()
        # create_shots()
    #
    # print df_shots.tail()
    # df_lines = get_lines()
    # df_lines = df_lines.convert_objects(convert_numeric=True)
    # print df_lines.head()
    # df_lines.to_hdf('lines.h5', 'df_lines', format='table', mode='w', complevel=6, complib='blosc')
    # create_bs()
