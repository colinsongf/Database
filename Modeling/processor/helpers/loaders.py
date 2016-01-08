import pandas as pd
import numpy as np


class DFHolder(object):
    '''
    DFHolder: Class that wraps around a collection of dataframes for easier
    access.

    Attributes:
    - lines: a dataframe from the OU data file
    - shots: Wrapper of xefg dataframes, accessible by shots.players,
    shots.teams, and shots.globals
    - boxscores: Same as shots but for boxscore dataframes
    '''
    def __init__(self, df_lines, shotswrapper, bswrapper):
        self.lines = df_lines
        self.shots = shotswrapper
        self.boxscores = bswrapper


class DataframeWrapper(object):
    def __init__(self, df_players, df_teams, df_globals):
        self.players = df_players
        self.teams = df_teams
        self.globals = df_globals


class BoxscoreDFWrapper(DataframeWrapper):
    def select_played_only(self):
        '''
        select_played_only: filter the player boxscore to contain only entries
        for players who played positive minutes
        '''
        self.players = self.players[self.players['MIN'] > 0]


class ShotsDFWrapper(DataframeWrapper):
    def select_current_season(self, year):
        pass


class Slicer(object):
    '''
    Slicer: Static class with methods that slice dataframes for a given season.
    '''
    def __init__(self):
        pass

    @staticmethod
    def slice_all_data(large_dfs, year):
        df_lines = Slicer.slice_lines(large_dfs.lines, year)
        df_shots = Slicer.slice_shots(large_dfs.shots, year)
        return DFHolder(df_lines, df_shots, None)

    @staticmethod
    def slice_shots(shots_dfs, year):
        df_player_xefg = Slicer.slice_df(shots_dfs.players, year, 'query')
        df_team_xefg = Slicer.slice_df(shots_dfs.teams, year, 'index')
        df_global_xefg = Slicer.slice_df(shots_dfs.globals, year, 'boolean')

        return ShotsDFWrapper(df_player_xefg, df_team_xefg, df_global_xefg)

    @staticmethod
    def slice_lines(df_lines, year):
        return Slicer.slice_df(df_lines, year, 'boolean')

    @staticmethod
    def slice_df(df, year, mode):
        '''
        Selects rows in dataframe corresponding to current season. Use index if
        season column in df index. Use query for df >10k rows, boolean otherwise
        '''
        curr_season = str(year)

        if (mode == 'index'):
            return df.loc[int(curr_season)]

        if (mode == 'query'):
            return df.query('Season == ' + curr_season)

        if (mode == 'boolean'):
            return df[df['Season'] == int(curr_season)]


class Loader(object):
    '''
    Loader: Static class with methods that load dataframes from disk.
    '''
    def __init__(self):
        pass

    @staticmethod
    def load_large_dfs():
        return DFHolder(Loader.load_lines(), Loader.load_shots(), None)

    @staticmethod
    def load_boxscores(year):
        '''
        load_boxscores: reads hdf storage files and returns boxscore dataframes for a season
        '''
        # forming strings to read in files
        year_indicator = str(year)[2:4]
        players_filepath = './data/players/bs' + year_indicator + '.h5'
        team_filepath = './data/teams/tbs' + year_indicator + '.h5'

        # load team and player boxscores
        df_bs = pd.read_hdf(players_filepath, 'df_bs')
        df_teams = pd.read_hdf(team_filepath, 'df_team_bs')
        # df_inactives = pd.read_hdf(inactives_filepath, 'df_inactives')

        # set indexes on dataframe for easier searching
        df_bs.set_index(['PLAYER_ID', 'Date'], inplace=True)
        df_bs.sort_index(inplace=True)
        df_teams.set_index(['TEAM_ID', 'Date'], inplace=True)
        df_teams.sort_index(inplace=True)

        return BoxscoreDFWrapper(df_bs, df_teams, None)  # , df_inactives

    @staticmethod
    def load_lines():
        df_lines = pd.read_hdf('./data/lines/lines.h5', 'df_lines')
        return df_lines[~np.isnan(df_lines.index.values)]

    @staticmethod
    def load_shots():
        # read in team, player, and global shots data
        xefg_path = './data/xefg/'
        df_team_xefg = pd.read_hdf(xefg_path + 'team_xefg.h5', 'df_team_xefg')
        df_player_xefg = pd.read_hdf(xefg_path + 'player_xefg.h5', 'df_player_xefg')
        df_global_xefg = pd.read_hdf(xefg_path + 'global_xefg.h5', 'df_global_xefg')

        df_team_xefg.sort_index(inplace=True)
        df_player_xefg.sort_index(inplace=True)
        df_global_xefg.sort_index(inplace=True)

        return ShotsDFWrapper(df_player_xefg, df_team_xefg, df_global_xefg)
