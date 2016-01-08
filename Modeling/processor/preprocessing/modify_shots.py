import pandas as pd
import numpy as np
import tables
import math
import time
import timeit

shot_zones = ['atb3', 'c3', 'mid', 'ra', 'paint']
player_sizes = ['big', 'small']
history_steps = 10
min_player_games = 1


def load_dataframes():
    folder_filepath = '/Users/RL/Dropbox/Public/NBA_data/xefg/'
    global_filepath = folder_filepath + 'global_xefg_by_date.csv'
    df_xefg = pd.read_csv(global_filepath, index_col=['player_size', 'Date'])

    team_filepath = folder_filepath + 'team_xefg.csv'
    df_team_xefg = pd.read_csv(team_filepath, index_col=['season', 'team_id', 'oord', 'size', 'Date'])

    player_filepath = folder_filepath + 'player_xefg.csv'
    df_player_xefg = pd.read_csv(player_filepath, index_col=['player_id', 'date'])

    lines_filepath = './data/lines/lines.h5'
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    return df_xefg, df_team_xefg, df_player_xefg, df_lines


def write_dataframe(df, name):
    filepath = './data/temp/' + name + '.h5'
    table_name = 'df_' + name
    df.to_hdf(filepath, table_name, format='table', mode='w', complevel=6, complib='blosc')


def create_xefg_dict(row, global_xefg_dict, colnames):
    size = row.name[0]
    curr_date = row.name[1]

    curr_dict = dict(zip([size + '_' + col for col in colnames], row.values))
    global_xefg_dict[curr_date].update(curr_dict)


def create_player_xefg_dict(row, player_xefg_dict, colnames, player_id):
    curr_date = row.name
    curr_dict = dict(zip(colnames, row.values))
    player_xefg_dict[player_id].update({curr_date: curr_dict})


def find_previous_date(player_dates, curr_date):
    return player_dates[np.searchsorted(player_dates, curr_date) - 1]


def create_team_xefg_dict(row, team_xefg_dict, colnames, team_id, size):
    curr_dict = dict(zip(colnames, row.values))
    team_xefg_dict[team_id][size].update({row['game_id']: curr_dict})

if __name__ == "__main__":
    start_time = time.time()

    df_xefg, df_team_xefg, df_player_xefg, df_lines = load_dataframes()

    print df_xefg.sample(5)
    # write_dataframe(df_xefg, 'global_xefg')


    '''
    df_player_xefg['Season'] = df_player_xefg['game_id'].astype(np.string_).str[0:3]
    df_player_xefg['Season'] = df_player_xefg['Season'].str.replace('20', '200')
    df_player_xefg['Season'] = df_player_xefg['Season'].str.replace('21', '201')
    df_player_xefg['Season'] = df_player_xefg['Season'].astype(int)
    df_player_xefg = df_player_xefg.sort_index().drop('player_name', axis=1)

    print df_player_xefg.head()
    write_dataframe(df_player_xefg, 'player_xefg')
    '''
    '''

    df_player_xefg = df_player_xefg.sort_index().drop('player_name', axis=1)

    player_ids = pd.unique(df_player_xefg.index.get_level_values('player_id'))
    player_shot_dates = dict.fromkeys(player_ids)
    player_xefg_dict = {key: {} for key in player_ids}

    colnames = [zone + '_pps' for zone in shot_zones]
    colnames = colnames + [zone + '_attempt' for zone in shot_zones]

    for player_id in player_ids:
        df_curr_player = df_player_xefg.loc[player_id]
        player_shot_dates[player_id] = df_curr_player.index.values

        df_xefg_sum = pd.rolling_sum(df_curr_player, window=history_steps, min_periods=min_player_games + 1)

        for zone in shot_zones:
            # calculate pps over summed window
            df_xefg_sum[zone + '_pps'] = df_xefg_sum[zone + '_pts'].div(df_xefg_sum[zone + '_attempt'])

        df_xefg_sum[colnames].apply((lambda row: create_player_xefg_dict(row, player_xefg_dict, colnames, player_id)), axis=1)

    print player_xefg_dict[15][39153]
    '''

    '''
    df_xefg = df_xefg.drop('Season', axis=1)

    df_xefg = df_xefg.loc[player_sizes]

    dates_list = df_xefg.index.get_level_values('Date')
    global_xefg_dict = {key: {} for key in dates_list}
    colnames = df_xefg.columns.values.tolist()

    df_xefg.apply(lambda row: (create_xefg_dict(row, global_xefg_dict, colnames)), axis=1)
    '''

    df_team_xefg.sort_index(inplace=True)

    write_dataframe(df_team_xefg, 'team_xefg')

    '''

    curr_season = 2003

    for curr_season in xrange(2003,2014):
        df_team_xefg_season = df_team_xefg.loc[curr_season]

        team_id_list = pd.unique(df_team_xefg_season.index.get_level_values('team_id'))
        team_xefg_dict = {key: {size: {} for size in player_sizes} for key in team_id_list}

        for team_id in team_id_list:
            df_curr_team_xefg = df_team_xefg_season.loc['defense', team_id]

            for size in player_sizes:
                df_team_xefg_sum = pd.rolling_sum(df_curr_team_xefg.loc[size], window=history_steps, min_periods=history_steps)

                df_team_xefg_sum['tot_attempt'] = df_team_xefg_sum[zonecols].sum(axis=1)

                for zone in shot_zones:
                    # calculate pps over summed window
                    df_team_xefg_sum[zone + '_pps'] = df_team_xefg_sum[zone + '_pts'].div(df_team_xefg_sum[zone + '_attempt'])

                    # calculate frequency of each shot type
                    df_team_xefg_sum[zone + '_freq'] = df_team_xefg_sum[zone + '_attempt'].div(df_team_xefg_sum['tot_attempt'])

                df_team_xefg_sum[df_team_xefg_sum == np.inf] = 0
                df_team_xefg_sum['game_id'] = df_team_xefg_sum['game_id'].shift(-1)

                colnames = [zone + '_pps' for zone in shot_zones]
                colnames = colnames + [zone + '_freq' for zone in shot_zones]
                colnames.append('tot_attempt')
                colnames.append('game_id')
                # print df_team_xefg_sum.head()
                df_team_xefg_sum[colnames].apply((lambda row: create_team_xefg_dict(row, team_xefg_dict, colnames, team_id, size)), axis=1)

            print str(int(team_id))

    # print team_xefg_dict[team_id_list[0]]['small'][203010548]
    '''

    print("--- %s seconds ---" % (time.time() - start_time))

    # write_dataframe(df_xefg, 'xefg')
