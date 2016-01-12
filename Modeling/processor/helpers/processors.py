import pandas as pd
import numpy as np
import utilities
import calculators
import data_objects
import df_manipulators
from numpy.random import randint
from loaders import Loader


class DataProcessor(object):
    '''
    DataProcessor: Parent class for BoxscoreDataProcessor and ShotsDataProcessor
    Initializes with a RollingCalculator object that will specify what timeframe to calculate moving averages over.
    '''
    def __init__(self, rollingcalculator):
        self.roller = rollingcalculator


class BoxscoreDataProcessor(DataProcessor):
    '''
    BoxscoreDataProcessor: This Processor object handles all processing tasks
    related to the boxscore dataframes.
    '''
    # drop these bs stats (since they replicate advanced stats)
    bs_stats_to_drop = ['AST', 'BLK', 'FG3A', 'FG3M', 'FGA', 'FGM', 'FTA', 'FTM', 'OREB', 'DREB', 'REB', 'STL', 'TO']

    def calc_league_stats(self, df_teams, dates):
        '''
        calc_league_stats(): Given a team boxscore df, calculates league
        stats including PPG, PACE, PPS.

        Return league_stats, a dict. Keys are dates, values are dicts of data
        corresponding to league stats PRIOR to that date. This will be used
        for advanced stat calculation
        '''
        # initialize output dict
        league_stats = {key: {} for key in dates}

        # iterate through dates
        for curr_date in dates:

            # select rows of dataframe previous to current date
            df_prev = df_teams[df_teams.index.get_level_values('Date') < curr_date]

            # sum the columns and convert to a dict
            league_sum = utilities.dict_column_sum(df_prev)

            # based on summed league boxscores, calc league stats
            league_stats[curr_date] = calculators.calc_league_stats(league_sum, len(df_prev.index))

        return league_stats

    def calc_sum_avg_stats(self, df_bs, dates):
        '''
        calc_sum_avg_stats(): Calculates for a given boxscore dataframe,
        rolling sums and averages of boxscore stats.

        Returns bs_sum, bs_avg which are dicts. Keys are dates, values are
        dicts with rolling data calculated THROUGH that date.
        '''
        bs_avg = {key: {} for key in dates}
        bs_sum = {key: {} for key in dates}

        # select desired columns by selecting columns with float datatype
        df_bs = df_bs.select_dtypes(include=[np.float64])

        # use RollingCalculator object to process dataframe
        self.roller.rolling_sum_to_dict(df_bs, bs_sum)
        self.roller.rolling_avg_to_dict(df_bs, bs_avg)

        return bs_sum, bs_avg

    def calc_team_opp_stats(self, df_teams_float, team_ids, opp_ids, dates):
        '''
        calc_team_opp_stats(): Calculates for a given player, rolling sums
        of team and opp stats in the games that they played in.

        Returns team_sum, opp_sum which are dicts. Keys are dates, values are
        dicts with rolling data calculated THROUGH that date.
        '''
        team_sum = {key: {} for key in dates}
        opp_sum = {key: {} for key in dates}

        # select dataframes based on zipepd dates, team/opp lists
        df_team = df_teams_float.loc[zip(team_ids, dates)]
        df_opp = df_teams_float.loc[zip(opp_ids, dates)]

        # use RollingCalculator object to process dataframes
        self.roller.rolling_sum_to_dict(df_team, team_sum)
        self.roller.rolling_sum_to_dict(df_opp, opp_sum)

        return team_sum, opp_sum

    def process_sum_avg_stats(self, bs_sum, bs_avg, team_sum, opp_sum, league_stats, bs_advanced):
        '''
        process_sum_avg_stats():
        1. Calculates advanced stats using player, team, and opponent stats.
        2. Fixes percentage stats in the avgs dict (ie changes them to fgm/fga
        instead of an average of percentages)
        3. Deletes keys in the averages dict for stats that overlap within
        advanced stats, as specified in the bs_stats_to_drop property.
        '''
        # iterate through all dates
        for date in bs_advanced:
            # calculate advanced stats
            bs_advanced[date] = self.calc_advanced_stats(bs_sum[date], team_sum[date], opp_sum[date], league_stats[date])

            # fix percentage stats for that date
            self.fix_percentage_stats(bs_sum[date], bs_avg[date])

            # delete unwanted boxscore stats
            utilities.del_keys_dict(self.bs_stats_to_drop, bs_avg[date])

    @staticmethod
    def fix_percentage_stats(plyr_sum, plyr_avg):
        '''
        fix_percentage_stats: fixes percentage stats by doing makes / attempts
        instead of an average of individual percentages.
        '''
        plyr_avg['PLUS_MINUS'] = plyr_sum['PLUS_MINUS']
        plyr_avg['FG_PCT'] = 0.0 if (plyr_sum['FGA'] == 0) else (plyr_sum['FGM'] / plyr_sum['FGA'])
        plyr_avg['FG3_PCT'] = 0.0 if (plyr_sum['FG3A'] == 0) else (plyr_sum['FG3M'] / plyr_sum['FG3A'])
        plyr_avg['FT_PCT'] = 0.0 if (plyr_sum['FTA'] == 0) else (plyr_sum['FTM'] / plyr_sum['FTA'])

    @staticmethod
    def calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats):
        return calculators.calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats)


class ShotsDataProcessor(DataProcessor):
    '''
    ShotsDataProcessor: Child class of DataProcessor. Handles all processing
    tasks related to the shots dataframes.

    Attributes:
    - roller: Initiates with a RollingCalculator object that is used for doing
    moving sums and averages
    - colnames: List of colnames that we want to output from the shots data.
    '''

    # class attributes laying out the structure of the xefg data
    oord = ('offense', 'defense')
    shot_zones = ('atb3', 'c3', 'mid', 'ra', 'paint')
    player_sizes = ('big', 'small')

    def __init__(self, rollingcalculator):
        DataProcessor.__init__(self, rollingcalculator)
        self.colnames = [zone + '_pps' for zone in ShotsDataProcessor.shot_zones] + [zone + '_freq' for zone in ShotsDataProcessor.shot_zones]
        self.colnames.append('tot_attempt')

    def calc_player_shots(self, df_player, shots_dates):
        '''
        calc_player_shots: Calculates rolling sums on player-level xefg data
        for a given player. Takes in df_player, a df_player_xefg dataframe
        sliced for a given player.

        Returns player_shots_dict. Keys are dates, values are dicts of summed
        data through that date.
        '''
        player_shots_dict = {key: {} for key in shots_dates}
        self.roller.rolling_shots_to_dict(df_player, player_shots_dict, self.colnames)

        return player_shots_dict

    def create_team_shots_data(self, df_team_shots, shots_dates):
        '''
        create_team_shots_data: takes in a dataframe with team_xefg data
        for a given team.

        Returns a list of ShotsData objects with attributes of
        offense or defense, and small/big/all. ShotsData.data contains a dict,
        with keys as dates and values as summed data through that date.
        '''
        shots_data = []
        # iterate through offense/defense, player sizes
        for side in self.oord:
            for size in self.player_sizes:
                # calculate summed stats then append ShotsData object for output
                shots_data.append(data_objects.ShotsData(side, size, self.calc_team_shots(df_team_shots.loc[side, size], shots_dates)))

        return shots_data

    def calc_team_shots(self, df_team, shots_dates):
        '''
        calc_team_shots: Called by create_team_shots to sum team-level xefg
        stats.
        '''
        team_shots_dict = {key: {} for key in shots_dates}

        # Use RollingCalculator object to calculate rolling sums
        self.roller.rolling_shots_to_dict(df_team, team_shots_dict, self.colnames)

        return team_shots_dict

    def calc_global_shots(self, df_global_xefg, shots_dates):
        '''
        calc_global_shots: Process the df_global_xefg dataframe.

        Returns a list of ShotsData objects.
        '''
        global_xefg_data = []

        # drop unwanted season variable
        df_global_xefg = df_global_xefg.drop(['Season'], axis=1)

        # iterate through sizes in player_sizes
        for size in self.player_sizes:

            global_shots_dict = {key: {} for key in shots_dates}
            # Create a ShotsData object containing global xefg data
            df_global_xefg.loc[size].apply(lambda row: df_manipulators.dict_from_row(row, global_shots_dict), axis=1)

            # append to output
            global_xefg_data.append(data_objects.ShotsData('offense', size, global_shots_dict))

        return global_xefg_data


class GameProcessor(object):
    '''
    GameProcessor: This Processor object essentially handles processing tasks
    related to looking up player, team, global variables for a specific game.
    It then collects all these variables and forms them into a single row.
    That row is then appended to the overall season output.

    It instantiates with some attributes given to it by the specified parameters
    in JobHandler.

    Attributes: All parameters specified by JobHandler object.
    - history_steps: Minimum number of previous games by each team before the
    game can be included.
    - min_player_games: Minimum number of games played by a player before
    they can be included in the output.
    - num_players: Number of players to include from each team roster.

    - active_lists: tuple (home, away) of lists of player_ids who were listed for each game
    - inactive_lists: same as active_lists but player_ids who were inactive


    '''

    num_starters = 5
    position_mappings = {'PG': 'Guard', 'SG': 'Wing', 'SF': 'Wing',
                         'PF': 'Big', 'C': 'Big', 'C-PF': 'Big', 'PF-C': 'Big',
                         'SF-SG': 'Wing', 'SG-SF': 'Wing', 'PF-SF': 'Wing',
                         'SF-PF': 'Wing', 'PG-SF': 'Wing', 'SG-PF': 'Wing',
                         'G-F': 'Wing', 'SG-PG': 'Guard', 'PG-SG': 'Guard',
                         'F': 'Big', 'C-SF': 'Big'}
    flex_mappings = {'SG': 'Guard', 'SF': 'Big', 'PF': 'Wing',
                     'SG-SF': 'Guard', 'SF-SG': 'Guard', 'SG-PF': 'Guard',
                     'PG-SG': 'Wing', 'SG-PG': 'Wing', 'SF-PF': 'Big'}
    flex_pos_mappings = position_mappings.copy()
    flex_pos_mappings.update(flex_mappings)

    def __init__(self, history_steps, min_player_games, num_players, bench_positions):
        self.history_steps = history_steps
        self.min_player_games = min_player_games
        self.num_players = num_players
        self.bench_positions = bench_positions

    def form_player_vars(self, starters, active_lists, inactive_lists, players, date):
        '''
        form_player_vars: Iterates through the player lists of the home_output
        and away teams. Returns a list of DataVars objects, with each element
        contaning a DataVars object with all the variables for one player

        If one of the rosters didn't meet the history criteria for sufficient
        player data it will return None instead. This game will then be skipped
        and won't be added to the final data output.
        '''
        # iterate through home player list
        home_output = self.process_roster(starters[0], active_lists[0], inactive_lists[0], players, date, 'home')

        # return blank output if history criteria not met
        if not home_output:
            return None

        # iterate through away player list
        away_output = self.process_roster(starters[1], active_lists[1], inactive_lists[1], players, date, 'away')

        # return blank output if history criteria not met
        if not away_output:
            return None

        return home_output + away_output

    def process_roster(self, starters, player_list, inactive_list, players, date, site):
        '''
        process_roster(): Goes through the home and away player lists.
        Extracts the desired sum, average boxscore/shots data from dicts
        contained within the corresponding Player object.

        Then collects all this data in DataVars objects and outputs them.
        '''
        starters_output = self.process_starters(starters, players, date, site)
        bench_output = self.process_bench(starters, player_list, inactive_list, players, date, site)

        if (starters_output is None) or (bench_output is None):
            return None

        return starters_output + bench_output

    def process_starters(self, starters, players, date, site):
        starters_output = []
        for num, player_id in enumerate(starters, start=1):
            player = players[player_id]

            # skip game if starter with insufficient history
            if not self.player_history_met(player, date, self.min_player_games):
                return None

            # get previous dates to use for accessing dicts
            last_played_date, last_shot_date = utilities.prev_date(player.dates, date), utilities.prev_date(player.shots_dates, date)

            # access the stats dicts from player object for previous game date
            curr_player_vars = self.create_player_vars(player, last_played_date, last_shot_date, site, 'start_p' + str(num))

            starters_output.append(curr_player_vars)

        if len(starters_output) is not 5:
            print 'WRONG NUMBER OF STARTERS'
            return None

        return starters_output

    def process_bench(self, starters, player_list, inactive_list, players, date, site):
        position_names = sorted(self.bench_positions.keys())
        bench_output = {key: [] for key in position_names}
        bench_output.update({'WC': []})

        # iterate through each player id on the roster
        for num, player_id in enumerate(player_list, start=1):

            # skip player if player was inactive or in the starting lineup
            if (player_id in inactive_list) or (player_id in starters):
                continue

            # get Player object for given player
            player = players[player_id]

            # skip player if insufficient history
            if not self.player_history_met(player, date, self.min_player_games):
                continue

            # get previous dates to use for accessing dicts
            last_played_date, last_shot_date = utilities.prev_date(player.dates, date), utilities.prev_date(player.shots_dates, date)

            # create player variables from dicts on previous date
            curr_player_vars = self.create_player_vars(player, last_played_date, last_shot_date, site, 'bench_p' + str(num))

            # determine player position and secondary player position
            mapped_position = self.position_mappings[player.position]
            flex_position = self.flex_pos_mappings[player.position]

            # add to list of players in the main position if not full
            if len(bench_output[mapped_position]) < self.bench_positions[mapped_position]:
                bench_output[mapped_position].append(curr_player_vars)
                continue
            # add to secondary position if that list is not full
            elif len(bench_output[flex_position]) < self.bench_positions[flex_position]:
                bench_output[flex_position].append(curr_player_vars)
            # otherwise hold in wildcard position
            else:
                bench_output['WC'].append(curr_player_vars)

        final_bench_output = []
        # fill in positions w/o enough players using wildcard positions
        for pos in position_names:
            max_num_at_position = self.bench_positions[pos]
            num_players_short = max_num_at_position - len(bench_output[pos])
            for _ in range(num_players_short):
                try:
                    bench_output[pos].append(bench_output['WC'].pop(0))
                except:
                    return None

            # if still short return an error
            if (len(bench_output[pos]) < max_num_at_position):
                print 'Still short after adding WCs'
                return None
            else:
                final_bench_output.extend(bench_output[pos])

        return final_bench_output

    def form_team_vars(self, home_team, away_team, date):
        '''
        form_team_vars: collects team-level variables by accessing Team objects
        for the home and away teams. Collects them into DataVars objects
        and returns them.
        '''

        # get previous dates by home and away teams
        home_prev_date, away_prev_date = utilities.prev_date(home_team.shots_dates, date), utilities.prev_date(away_team.shots_dates, date)

        # initialize DataVars object
        team_vars = data_objects.DataVars()

        # go through lists of ShotsData objects and add them into DataVars
        for shots_item in home_team.shots_data:
            team_vars.add_shots_data(shots_item, home_prev_date, 'home')
        for shots_item in away_team.shots_data:
            team_vars.add_shots_data(shots_item, away_prev_date, 'away')

        return team_vars

    def form_global_vars(self, global_list, date):
        # initialize DataVars object with 'global' prefix
        global_vars = data_objects.DataVars('global')

        # go through lsit of ShotsData object and add into a DataVars object
        # for shots_item in global_list:
            # global_vars.add_shots_data(shots_item, date)

        return global_vars

    def form_game_vars(self, row):
        '''
        form_game_vars: Collects game-level info and aggregates it into
        a DataVars object.
        '''
        # randomizes result of cover and O/U if it was a push, otherwise 1 or 0
        y = randint(0, 1) if row['ATSr'] == 'P' else int(row['ATSr'] == 'W')
        over = randint(0, 1) if row['OUr'] == 'P' else int(row['OUr'] == 'O')

        # gets whether the spread was a push
        push = int(row['ATSr'] == 'P')

        # gets the line
        line = float(row['Line'])

        home_rest, away_rest = self.get_rest(row)
        home_id, away_id = self.get_team_ids(row)
        game_id = row.name
        total = row['Total']

        # collects info into a dict
        datum = {'y': y, 'over': over, 'LINE': line, 'push': push, 'home_rest': home_rest, 'away_rest': away_rest, 'home_id': home_id, 'away_id': away_id, 'game_id': game_id, 'total': total}

        # converts dict into a DataVars object
        game_vars = data_objects.DataVars()
        game_vars.add_dict(datum)

        return game_vars

    @staticmethod
    def create_player_vars(player, played_date, shot_date, site, prefix):
        plyr_avg = player.bs_avg[played_date]
        plyr_advanced = player.bs_advanced[played_date]
        plyr_shots = player.shots_dict[shot_date]

        # initialize storage variable for player vars with string prefix
        curr_player_vars = data_objects.DataVars(site + '_' + prefix)

        # Add all data dicts to the DataVars object
        for data_dict in [plyr_avg, plyr_advanced, plyr_shots]:
            curr_player_vars.add_dict(data_dict)

        return curr_player_vars

    @staticmethod
    def player_history_met(player, date, min_games):
        # count number of history games for current player
        game_count = utilities.count_prev_games(player.dates, date)
        shot_count = utilities.count_prev_games(player.shots_dates, date)

        return (min(game_count, shot_count) >= min_games)

    @staticmethod
    def count_prev_games(home_team, away_team, date):
        '''
        count_prev_games: returns the number of previous games played by each
        team. Used to check history criteria.
        '''
        return min(utilities.count_prev_games(home_team.dates, date), utilities.count_prev_games(away_team.dates, date))

    @staticmethod
    def find_prev_dates(home_team, away_team, date):
        home_prev_date = utilities.prev_date(home_team.dates, date)
        away_prev_date = utilities.prev_date(away_team.dates, date)
        return home_prev_date, away_prev_date

    @staticmethod
    def get_team_ids(row):
        return row['home_id'], row['away_id']

    @staticmethod
    def get_rest(row):
        rest_array = row['Rest'].split('&')
        home_rest = 0 if rest_array[0] == '' else rest_array[0]
        away_rest = 0 if rest_array[1] == '' else rest_array[1]
        return home_rest, away_rest


class ObjectProcessor(object):
    def __init__(self):
        pass


class PlayerProcessor(ObjectProcessor):
    @staticmethod
    def process(player):
        PlayerProcessor.process_boxscores(player)
        PlayerProcessor.process_shots(player)

    @staticmethod
    def process_boxscores(player):
        # calculate rolling sum and average stats
        player.bs_sum, player.bs_avg = player.bs_processor.calc_sum_avg_stats(player.boxscore, player.dates)

        # calculate rolling sum for team and opp stats
        player.team_sum, player.opp_sum = player.bs_processor.calc_team_opp_stats(player.team_bs_float, player.team_ids, player.opp_ids, player.dates)

        # fixes percentages and creates advanced stats
        player.bs_processor.process_sum_avg_stats(player.bs_sum, player.bs_avg, player.team_sum, player.opp_sum, player.data.boxscores.globals, player.bs_advanced)

    @staticmethod
    def process_shots(player):  # performs processing related to shots data
        # only execute if shots data is not blank
        if (player.shots is not None):
            # create shots_dict data object to store processed shots data
            player.shots_dict = player.shots_processor.calc_player_shots(player.shots, player.shots_dates)


class TeamProcessor(ObjectProcessor):
    @staticmethod
    def process(team):
        TeamProcessor.process_shots(team)
        TeamProcessor.process_boxscores(team)

    @staticmethod
    def process_shots(team):  # process xefg data
        # creates list of ShotsData objects to store shots data in
        team.shots_data = team.shots_processor.create_team_shots_data(team.shots, team.shots_dates)

    @staticmethod
    def process_boxscores(team):
        # creates dicts with team/opp sum and avg stats
        team.team_sum, team.team_avg = team.bs_processor.calc_sum_avg_stats(team.boxscore, team.dates)
        team.opp_sum, team.opp_avg = team.bs_processor.calc_sum_avg_stats(team.opp_boxscore, team.dates)

        # calculate advanced team possesion and pace stats
        team.team_poss = {date: calculators.calc_poss(team.team_sum[date], team.opp_sum[date]) for date in team.dates}
        team.team_pace = {date: calculators.calc_pace(team.team_sum[date], team.team_poss[date], team.team_poss[date]) for date in team.dates}


class SeasonProcessor(object):
    def __init__(self):
        pass

    @staticmethod
    def process(season):
        SeasonProcessor.create_player_lists(season.teams, season.year)

    @staticmethod
    def create_player_lists(teams, year):
        '''
        create_player_lists: Returns a dataframe with a column DND_IDS, which contains a list of player ids for inactive players.

        Inactive players are defined as players who were listed as DND (did not dress) or NWT (not with team)
        '''
        df_bs = Loader.load_unsorted_boxscore(year)
        # df_inactives = df_inactives[~np.isnan(df_inactives['Date'])]

        # create dataframes with players who DND and NWT
        df_dnd = df_bs[df_bs['COMMENT'].str.contains('DND', na=False)]
        df_without_dnd = df_bs[~df_bs['COMMENT'].str.contains('DND', na=False)]
        df_nwt = df_without_dnd[df_without_dnd['COMMENT'].str.contains('NWT', na=False)]

        # populate list with all player IDs in boxscore
        df_bs.apply((lambda row:         teams[row['TEAM_ID']].player_list[row['Date']].append(row['PLAYER_ID'])), axis=1)

        # populate list with all player IDs who DND
        df_dnd.apply((lambda row: teams[row['TEAM_ID']].inactive_list[row['Date']].append(row['PLAYER_ID'])), axis=1)

        # populate list with all player IDs who were NWT
        df_nwt.apply((lambda row: teams[row['TEAM_ID']].inactive_list[row['Date']].append(row['PLAYER_ID'])), axis=1)
