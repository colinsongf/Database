import pandas as pd
import numpy as np
from helpers.loaders import Loader, Slicer
from helpers.processors import SeasonProcessor, BoxscoreDataProcessor, ShotsDataProcessor, GameProcessor
from helpers.data_objects import DataVars


class Season(object):
    '''
    Season: This class represents data from a single season. Contains player, team, and game objects corresponding to that season.

    It initializes with a tuple of DataProcessor objects, used to process dataframes for boxscore and shots data.

    JobHandler uses Season object to perform this series of processing steps:
    1. Initializes season
    2. Calculates global data, such as league stats and player lists.
    3. Creates team and player objects for each team and player who played in the current season
    4. Processes data for team and player objects, ie calculating moving averages
    5. Creates game objects containing data corresponding to each game
    6. Uses game objects to generate output with one row of variables per game

    Attributes
    - players: a dict with key: value pairs of player_ids: Player objects.
    - teams: a dict with k:v pairs of team_ids: Team objects.
    - games: a list of Game objects.
    - year: current season, ie 2003
    - data: DFHolder object containing all dataframes. Hierarchy is
    (boxscore/shots) -> (player/teams/globals). For example team boxscore data
    is in self.data.boxscores.teams
    - dates: All dates on which a game was played, based on boxscore data
    - shots_dates: All dates on which a shot was taken, based on shots data
    - _processor: DataProcessor objects. To be passed into sub-classes (player,
     team, game) for processing data
    '''
    def __init__(self, year, large_dfs, processors):
        self.year = year

        # Select current season of all dataframes
        self.data = Slicer.slice_all_data(large_dfs, self.year)
        self.data.boxscores = Loader.load_boxscores(self.year)

        # Create lists of dates, used for indexing and access
        self.dates = pd.unique(self.data.boxscores.teams.index.get_level_values('Date').values)
        self.shots_dates = pd.unique(self.data.shots.globals.index.get_level_values('Date').values)

        # initialize processor classes to use for processing data
        self.boxscore_processor, self.shots_processor, self.game_processor = processors

    def process_globals(self):  # processing for global stats
        # calculate league wide stats, ie pace
        self.data.boxscores.globals = self.boxscore_processor.calc_league_stats(self.data.boxscores.teams, self.dates)

        # calculate global xefg by zone
        self.global_xefg = self.shots_processor.calc_global_shots(self.data.shots.globals, self.shots_dates)

    def generate_output(self):  # creating a final season output object
        final_header, season_output = [], []

        for game in self.games:  # iterate through each game

            # create a row of data for each game
            game_row = game.process_game()
            if game_row:
                season_output.append(game_row.values)

        # set header if header not blank
        if game_row:
            final_header = game_row.header

        return final_header, season_output

    def create_objects(self):  # initializes player, team, game objects
        '''
        IMPORTANT: team objects must be initialized first, so that player lists
        can be created before the player boxscore is filtered for entries
        with players who played more than 0 minutes
        '''
        # create team, player objects
        self.create_team_objects()
        self.create_player_objects()

        # init blank list of game objects
        self.games = []

    def create_player_objects(self):
        # Initialize list of all player ids
        player_ids = pd.unique(self.data.boxscores.players.index.get_level_values('PLAYER_ID').values)

        # Filter dataframe for MIN > 0
        self.data.boxscores.select_played_only()

        self.players = {player_id: Player(player_id, self.data, self.boxscore_processor, self.shots_processor) for player_id in player_ids}

        # populate shots data (separate b/c of conflicts in players w/ 0 shots)
        self.set_player_shot_data()

    def create_team_objects(self):  # creates self.teams
        self.teams = {team_id: Team(team_id, self.data, self.boxscore_processor, self.shots_processor) for team_id in pd.unique(self.data.boxscores.teams.index.get_level_values('TEAM_ID').values)}

        # create active, inactive player lists within each team object
        SeasonProcessor.create_player_lists(self.data.boxscores.players, self.teams)

    def create_game_objects(self):  # create game objects with lines dataframe
        self.data.lines.apply((lambda row: self.games.append(Game(row, self.players, self.teams, self.global_xefg, self.game_processor))), axis=1)

    def process_objects(self):
        # process all boxscore data at player level
        for player_id, player in self.players.iteritems():
            player.process_boxscores()

        # process all shots data at team level
        for team_id, team in self.teams.iteritems():
            team.process_shots()

    def set_player_shot_data(self):  # creates shot data within player objects
        for player_id in pd.unique(self.data.shots.players.index.get_level_values('player_id').values):  # for all players who took a shot

            # initialize shots data
            self.players[player_id].set_shots_data()

            # perform processing tasks
            self.players[player_id].process_shots()


class Game(object):
    '''
    Game: This class is instantiated using a row of the OU data lines dataframe.
    It then goes through a series of processing tasks.

    1. Gets the last date that the home and away teams played on.
    Uses this for index access.
    2. Gets home and away active and inactive player lists.
    3. Goes through the player lists and collects player-level variables from
    the corresponding Player objects.
    4. Collects team level, global level objects from corresponding objects.
    5. Collects game variables from the row, ie the line.
    6. Using process_game(), places all data in DataVars objects.
    Then outputs values in a single row.
    7. The Season object then collects all these rows and outputs them to
    to eventually be formed into a pandas dataframe for final output.

    Attributes
    - id: game_id
    - date: date the game was played on

    - players: Dict with player_ids keyed to Player objects.
    - teams: Same as players but with teams
    - globals: object containing global xefg data.
    - processor: GameProcessor object that does all the processing tasks.

    - prev_games: Counts number of games played by whichever team played fewer.
    Used to verify that history requirement was satisfied.
    - active_lists, inactive_lists: home and away lists of player ids.
    '''
    def __init__(self, row, players, teams, global_data, processor):
        # gets row from dataframe (as a copy to avoid future access conflict)
        self.row = row.copy()

        # get game_id
        self.id = self.row.name
        self.date = self.row['Date']

        # instantiate dicts of player and team objects
        self.players = players
        self.teams = teams
        self.globals = global_data

        # GameProcessor object
        self.processor = processor

        # get home and away team ids, then home and away Team objects
        self.home_id, self.away_id = GameProcessor.get_team_ids(self.row)
        self.home_team, self.away_team = self.teams[self.home_id], self.teams[self.away_id]

        # find previous date played by each team
        self.home_prev_date, self.away_prev_date = GameProcessor.find_prev_dates(self.home_team, self.away_team, self.date)

        # count number of previous games played by teams
        self.prev_games = GameProcessor.count_prev_games(self.home_team, self.away_team, self.date)

        # find active and inactive player lists for current game in Team objects
        self.create_player_lists()

    def __repr__(self):  # string representation of object with print()
        return str(self.id) + ': ' + str(self.home_id) + 'vs. ' + str(self.away_id)

    def process_game(self):

        # return blank output if insufficient games played
        if (self.prev_games < self.processor.history_steps):
            return None

        # aggregate player-level variables for both teams
        player_vars = self.processor.form_player_vars(self.player_lists, self.inactive_lists, self.players, self.date)

        # if player history criteria not met, return blank output
        if not player_vars:
            return None

        # aggregate team-level variables
        team_vars = self.processor.form_team_vars(self.home_team, self.away_team, self.date)

        # aggregate global level variables
        global_vars = self.processor.form_global_vars(self.globals, self.date)
        game_vars = self.processor.form_game_vars(self.row)

        # collect all variables into one row for output
        final_row = DataVars()
        final_row.add_lists_of_datavars(player_vars, team_vars, global_vars, game_vars)

        return final_row

    def create_player_lists(self):
        # access active and inactive lists for current game
        self.player_lists = [self.home_team.player_list[self.date], self.away_team.player_list[self.date]]
        self.inactive_lists = [self.home_team.inactive_list[self.date], self.away_team.inactive_list[self.date]]


class Player(object):

    '''
    Player: This class represents a player within a specific season.
    Contains all data related to that player, ie summed/averaged stats.

    Attributes
    - id: player_id

    - bs_sum: Dict with k:v pairs of date: dict of a rolling sum of boxscore data through that date.
    - bs_avg: Same as bs_sum but with a rolling average.
    - team_sum: Similar to bs_sum but with team level data. Used for advanced stats calculation.
    - opp_sum: Same as team_sum but with opposing team data.
    - bs_advanced: Same as bs_sum, but with calculated advanced stats.
    - shots_dict: Same as bs_sum but with summed xefg stats.

    - data: DFHolder inherited from Season. Used for access to raw dataframes
    - boxscore: df_bs sliced for current player
    - shots: df_player_xefg sliced for current player

    - dates: All dates where player played positive minutes. Used for indexing
    - shots_dates: All dates where player took a shot. Used for indexing
    - team_ids: List of team_ids. Zipped with dates for dataframe selection.
    - opp_ids: List of opposing team ids. Zipped with dates for df selection.
    - bs_processor: DataProcessor object inherited from Season
    - shots_processor: ShotsProcessor object inherited from Season
    '''
    def __init__(self, id, data, bs_processor, shots_processor):
        self.id = id
        self.data = data

        # initialize processor objects
        self.bs_processor = bs_processor
        self.shots_processor = shots_processor

        # slice boxscore dataframe for current player
        self.boxscore = self.data.boxscores.players.loc[self.id]
        self.dates = self.boxscore.index.values

        # select float type cols from team boxscore, for rolling sum calculation
        self.team_bs_float = self.data.boxscores.teams.select_dtypes(include=[np.float64])

        # get team and opp ids, used for dataframe access
        self.team_ids = self.boxscore['TEAM_ID'].values
        self.opp_ids = self.boxscore['OPP_ID'].values

        # initialize blank data objects
        self.shots = None
        self.shots_dates = []

        self.bs_advanced = {key: {} for key in self.dates}

    def set_shots_data(self):  # populate shots data

        # slice df_player_xefg for current player
        self.shots = self.data.shots.players.loc[int(self.id)]
        self.shots_dates = self.shots.index.values

    def process_boxscores(self):  # performs processing related to boxscores
        # calculate rolling sum and average stats
        self.bs_sum, self.bs_avg = self.bs_processor.calc_sum_avg_stats(self.boxscore, self.dates)

        # calculate rolling sum for team and opp stats
        self.team_sum, self.opp_sum = self.bs_processor.calc_team_opp_stats(self.team_bs_float, self.team_ids, self.opp_ids, self.dates)

        # fixes percentages and creates advanced stats
        self.bs_processor.process_sum_avg_stats(self.bs_sum, self.bs_avg, self.team_sum, self.opp_sum, self.data.boxscores.globals, self.bs_advanced)

    def process_shots(self):  # performs processing related to shots data
        # create shots_dict data object to store processed shots data
        self.shots_dict = self.shots_processor.calc_player_shots(self.shots, self.shots_dates)


class Team(object):
    '''
    Team: This class represents a team within a season.
    Contains all processed data related to that team.

    Attributes
    - id: team_id
    - data: Raw data inherited from Season.
    - processor: DataProcessor objects inherited from Season.

    - boxscore: df_bs sliced for current team
    - shots: df_team_xefg sliced for current team

    - player_list: Dict w/ Key:value of Date: player_ids list
    - inactive_list: Same as player_list but with players who DND or NWT

    - dates: List of all dates a team played on. Used for indexing/access
    - shots_dates: Same as dates but for the shots data

    - games: List of game ids that a team played in
    '''

    def __init__(self, id, data, bs_processor, shots_processor):
        self.id = id
        self.data = data
        self.bs_processor = bs_processor
        self.shots_processor = shots_processor

        # slice boxscore dataframe for current team
        self.boxscore = self.data.boxscores.teams.loc[self.id]
        self.boxscore_float = self.boxscore.select_dtypes(include=[np.float64])

        # slice shots dataframe for current team
        self.shots = self.data.shots.teams.loc[int(self.id)]

        # get dates for indexing
        self.dates = self.boxscore.index.get_level_values('Date').values
        self.shots_dates = self.shots.index.get_level_values('Date').values

        # get list of game ids
        self.games = self.boxscore['GAME_ID'].values

        # initialize blank player lists
        self.player_list = {date: [] for date in self.dates}
        self.inactive_list = {date: [] for date in self.dates}

    def process_shots(self):  # process xefg data
        self.shots_data = self.shots_processor.create_team_shots_data(self.shots, self.shots_dates)
