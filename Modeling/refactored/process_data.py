import pandas as pd
import numpy as np
import tables
import math
import time
import timeit
import collections
import pickle
import sys

min_year, max_year = 3, 13  # years to iterate over ie 3, 14 means 2003-2014
history_steps = 8  # num of games back to use for stats
min_player_games = 1  # num of games each player has to play at minimum
num_players = 9  # number of players to use from roster
shot_zones = ['atb3', 'c3', 'mid', 'ra', 'paint']  # zones in xefg data
player_sizes = ['all', 'small']  # sizes in xefg data

'''
Helper functions for numeric options on pandas dataframes for performance reasons
'''


def dict_column_sum(df_sum):
    '''
    dict_column_sum: helper function that sums the columns in a pandas dataframe and returns them to a dict

    Note: Significantly faster than pd.sum(axis=0).to_dict() for league stats, but not for player stats. Not sure why
    '''
    stats_header = df_sum.columns.values.tolist()
    dict_sum = dict(zip(stats_header, [df_sum[col].values.sum(axis=0) for col in stats_header]))
    return dict_sum


'''
Functions that get data from the lines dataframe
'''


def get_result(row):
    '''
    get_result: gets the line and ATS result from the lines df
    '''
    y = 1 if row['ATSr'] == 'W' else 0
    push = 1 if row['ATSr'] == 'P' else 0

    line = float(row['Line'])

    return y, push, line


def get_rest(row):
    '''
    get_rest: gets the home and away rest from the lines df
    '''
    rest_array = row['Rest'].split('&')
    home_rest = 0 if rest_array[0] == '' else rest_array[0]
    away_rest = 0 if rest_array[1] == '' else rest_array[1]

    return home_rest, away_rest


def get_team_ids(row):
    '''
    get_team_ids: get home and away ids from the lines df
    '''
    home_team = row['home_id']
    away_team = row['away_id']

    return home_team, away_team


'''
Functions that load dataframes and select parts of large dataframes
'''


def load_boxscores(year):
    '''
    load_boxscores: reads hdf storage files and returns boxscore dataframes for a season
    '''
    # forming strings to read in files
    year_indicator = "%02d" % (year,)

    players_filepath = './data/players/bs' + year_indicator + '.h5'
    team_filepath = './data/teams/tbs' + year_indicator + '.h5'
    # inactives_filepath = './data/inactives/inactives' + year_indicator + '/h5'

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')
    # df_inactives = pd.read_hdf(inactives_filepath, 'df_inactives')

    return df_bs, df_teams  # , df_inactives


def load_dataframes():
    '''
    Loads dataframes that cover multiple seasons, ie for shots data, lines from hdf files
    '''

    # read in lines data from OU file
    lines_filepath = './data/lines/lines.h5'
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')

    # read in team, player, and global shots data
    team_xefg_filepath = './data/xefg/team_xefg.h5'
    df_team_xefg = pd.read_hdf(team_xefg_filepath, 'df_team_xefg')

    player_xefg_filepath = './data/xefg/player_xefg.h5'
    df_player_xefg = pd.read_hdf(player_xefg_filepath, 'df_player_xefg')

    global_shots_filepath = './data/xefg/global_xefg.h5'
    df_global_xefg = pd.read_hdf(global_shots_filepath, 'df_global_xefg')

    return df_lines, df_player_xefg, df_team_xefg, df_global_xefg


def slice_df(df, year, mode):
    '''
    Selects rows in dataframe corresponding to current season. Use index if
    season column in df index. Use query for df >10k rows, boolean otherwise
    '''
    curr_season = '20' + "%02d" % (year,)

    if (mode == 'index'):
        return df.loc[int(curr_season)]

    if (mode == 'query'):
        return df.query('Season == ' + curr_season)

    if (mode == 'boolean'):
        return df[df['Season'] == int(curr_season)]

'''
Functions to calculate stats
'''


def player_bs_dict_from_row(row, player_dict, colnames, player_id):
    '''
    player_bs_dict_from_row: Helper function to be used with df.apply() to create bs_sum and bs_avg dicts. Populates a dict with key being the date, and value being a dict of data.
    '''
    curr_date = row.name
    curr_dict = dict(zip(colnames, row.values))
    player_dict[player_id].update({curr_date: curr_dict})


def team_bs_dict_from_row(row, player_dict, colnames, player_id):
    '''
    team_bs_dict_from_row: Helper function to be used with df.apply() to create bs_team_sum and bs_opp_sum dicts. Populates a dict with key being the date, and value being a dict of data.
    '''
    curr_date = row.name[1]
    curr_dict = dict(zip(colnames, row.values))
    player_dict[player_id].update({curr_date: curr_dict})


def calc_sum_avg_stats(df_bs, df_teams):
    '''
    calc_sum_avg_stats: Takes in player and team boxscore dataframes. Returns the dicts player_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum.

    player_dates: A dict with keys=player_ids, values=list of all dates a player played nonzero minutes on.

    bs_sum, bs_avg: A 2d dict with keys=player_ids and dates, values=dict of summed/averaged stats over the pre-set history window of n games.

    bs_team_sum, bs_opp_sum: A 2d dict with keys=player_ids and dates, values = dict of summed stats for a given player's teams and opposing teams over the history window.
    '''
    # get list of unique player ids
    player_ids = pd.unique(df_bs.index.get_level_values('PLAYER_ID'))

    # filter for entries in boxscore with nonzero min played
    df_bs_played = df_bs[df_bs['MIN'] > 0]

    # initializing dicts to store player sum, average data
    bs_sum = {key: {} for key in player_ids}
    bs_avg = {key: {} for key in player_ids}

    # initializing dicts to store dates, team, and opp lists
    player_team_list = dict.fromkeys(player_ids)
    player_opp_list = dict.fromkeys(player_ids)
    player_bs_dates = {key: [] for key in player_ids}

    # initializing dicts to store team and opp sum data for advanced stat calcs
    bs_team_sum = {key: {} for key in player_ids}
    bs_opp_sum = {key: {} for key in player_ids}

    for player_id in player_ids:  # iterating through all player ids

        # select entries for current player
        df_player = df_bs_played.loc[player_id]

        # store list of dates on which curr player had games
        player_bs_dates[player_id] = df_player.index.values

        # store the list of teams a player played for and against
        player_team_list[player_id] = df_player['TEAM_ID'].values
        player_opp_list[player_id] = df_player['OPP_ID'].values

        # select only numeric float columns to sum
        df_player = df_player.select_dtypes(include=[np.float64])

        # calculates rolling sum over given history window
        df_sums = pd.rolling_sum(df_player, window=history_steps, min_periods=min_player_games)

        # calculates rolling average over given history window
        df_avg = pd.rolling_mean(df_player, window=history_steps, min_periods=min_player_games)

        # get list of column labels
        colnames = df_sums.columns.values.tolist()

        # populate dicts for player sum and average data using rolling dfs
        df_sums.apply((lambda row: player_bs_dict_from_row(row, bs_sum, colnames, player_id)), axis=1)
        df_avg.apply((lambda row: player_bs_dict_from_row(row, bs_avg, colnames, player_id)), axis=1)

        # select only numeric float type columns to sum
        df_teams_float = df_teams.select_dtypes(include=[np.float64])

        # create team and opp dataframes based on dates, team/opp lists
        df_team = df_teams_float.loc[zip(player_team_list[player_id], player_bs_dates[player_id])]
        df_opp = df_teams_float.loc[zip(player_opp_list[player_id], player_bs_dates[player_id])]

        # calculate rolling sum for team and opp stats
        df_team_sum = pd.rolling_sum(df_team, window=history_steps, min_periods=min_player_games)
        df_opp_sum = pd.rolling_sum(df_opp, window=history_steps, min_periods=min_player_games)

        # get column labels
        colnames = df_team_sum.columns.values.tolist()

        # populate team and opp sum dicts for later advanced stat calculation
        df_team_sum.apply((lambda row: team_bs_dict_from_row(row, bs_team_sum, colnames, player_id)), axis=1)

        df_opp_sum.apply((lambda row: team_bs_dict_from_row(row, bs_opp_sum, colnames, player_id)), axis=1)

    return player_bs_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum


def calc_league_stats(game_id, df_teams):
    '''
    calc_league_stats: calculates league stats based on all games played so far in a season. Returns a dict
    '''
    # sum dataframe columns and return a dict
    # league_sum = df_teams.sum(axis=0).to_dict()
    league_sum = dict_column_sum(df_teams)

    league_stats = {}

    # initialize team stats for easier formulas
    TM_FGA, TM_FTA = league_sum['FGA'], league_sum['FTA']
    TM_FGM, TM_TOV = league_sum['FGM'], league_sum['TO']
    TM_ORB, TM_DRB = league_sum['OREB'], league_sum['DREB']
    OP_FGA, OP_FTA, OP_FGM, OP_ORB, OP_DRB, OP_TOV = TM_FGA, TM_FTA, TM_FGM, TM_ORB, TM_DRB, TM_TOV

    league_possessions = 0.5 * ((TM_FGA + 0.4 * TM_FTA - 1.07 * (TM_ORB / (TM_ORB + OP_DRB)) * (TM_FGA - TM_FGM) + TM_TOV) + (OP_FGA + 0.4 * OP_FTA - 1.07 * (OP_ORB / (OP_ORB + TM_DRB)) * (OP_FGA - OP_FGM) + OP_TOV))

    num_games = len(df_teams.index) / 2
    league_stats['PPG'] = league_sum['PTS'] / num_games
    league_stats['PACE'] = 48 * ((league_possessions * 2) / (2 * (league_sum['MIN'] / 5)))

    league_stats['PPS'] = league_sum['PTS'] / (league_sum['FGA'] + 0.44 * league_sum['FTA'])

    return league_stats


def calc_advanced_stats(plyr_sum, team_sum, opp_sum, player_id, league_stats):
    '''
    calc_advanced_stats: calculates all advanced stats based on the sum stats of a player, player's teams, and player's opponents. Returns a dict
    '''
    # initialize vars for easier formulas
    TM_MP, OP_MP = team_sum['MIN'], opp_sum['MIN']

    TM_FGA, TM_FTA = team_sum['FGA'], team_sum['FTA']
    TM_FGM, TM_FTM = team_sum['FGM'], team_sum['FTM']
    TM_3PA, TM_3PM = team_sum['FG3A'], team_sum['FG3M']
    TM_ORB, TM_DRB = team_sum['OREB'], team_sum['DREB']
    TM_FGM, TM_TOV = team_sum['FGM'], team_sum['TO']
    TM_AST, TM_BLK = team_sum['AST'], team_sum['BLK']
    TM_STL, TM_PF = team_sum['STL'], team_sum['PF']
    TM_PTS = team_sum['PTS']

    OP_FGA, OP_FTA = opp_sum['FGA'], opp_sum['FTA']
    OP_FGM, OP_FTM = opp_sum['FGM'], opp_sum['FTM']
    OP_3PA, OP_3PM = opp_sum['FG3A'], opp_sum['FG3M']
    OP_ORB, OP_DRB = opp_sum['OREB'], opp_sum['DREB']
    OP_FGM, OP_TOV = opp_sum['FGM'], opp_sum['TO']
    OP_AST, OP_BLK = opp_sum['AST'], opp_sum['BLK']
    OP_STL, OP_PF = opp_sum['STL'], opp_sum['PF']
    OP_PTS = team_sum['PTS']

    TM_POS = calc_poss(team_sum, opp_sum)
    OP_POS = calc_poss(opp_sum, team_sum)
    TM_PACE = calc_pace(team_sum, TM_POS, OP_POS)

    # OP_PACE = calc_pace(opp_sum, team_sum)

    # initialize output dictionary with player id
    # plyr_advanced = {'PLAYER_ID': player_id}
    plyr_advanced = {}

    # adv stat: possession and pace
    plyr_advanced['PACE'] = TM_PACE

    # adv stat: true shot percentage
    if plyr_sum['FGA'] + plyr_sum['FTA'] == 0.0:
        plyr_advanced['TS'] = 0.0
    else:
        plyr_advanced['TS'] = plyr_sum['PTS'] / (2*(plyr_sum['FGA'] + 0.44*plyr_sum['FTA']))

    # adv stat: 3 pointers attempt rate
    if plyr_sum['FGA'] == 0.0:
        plyr_advanced['3PAr'] = 0.0
    else:
        plyr_advanced['3PAr'] = plyr_sum['FG3A'] / plyr_sum['FGA']

    # adv stat: free throw attempt rate
    if plyr_sum['FGA'] == 0.0:
        plyr_advanced['FTr'] = 0.0
    else:
        plyr_advanced['FTr'] = plyr_sum['FTA'] / plyr_sum['FGA']

    # adv stat: offensive rebounding rate
    plyr_advanced['ORBr'] = (plyr_sum['OREB']*(TM_MP/5))/(plyr_sum['MIN']*(TM_ORB+OP_ORB))

    # adv stat: defensive rebounding rate
    plyr_advanced['DRBr'] = (plyr_sum['DREB']*(TM_MP/5))/(plyr_sum['MIN']*(TM_DRB+OP_DRB))

    # adv stat: total rebounding rate
    TRB = plyr_sum['OREB'] + plyr_sum['DREB']
    TM_TRB = TM_DRB + TM_ORB
    OP_TRB = OP_DRB + OP_ORB
    plyr_advanced['TRBr'] = (TRB*(TM_MP/5))/(plyr_sum['MIN']*(TM_TRB+OP_TRB))

    # adv stat: assist rate
    plyr_advanced['ASTr'] = plyr_sum['AST']/(((plyr_sum['MIN']/(TM_MP/5))*TM_FGM)-plyr_sum['FGM'])

    # adv stat: steal rate
    plyr_advanced['STLr'] = (plyr_sum['STL']*(TM_MP/5))/(plyr_sum['MIN']*OP_POS)

    # adv stat: blocking rate
    plyr_advanced['BLKr'] = (plyr_sum['BLK']*(TM_MP/5))/(plyr_sum['MIN']*(OP_FGA - OP_3PA))

    # adv stat: turnover rate
    if plyr_sum['FGA'] + plyr_sum['FTA'] + plyr_sum['TO'] == 0.0:
        plyr_advanced['TOVr'] = 0.0
    else:
        plyr_advanced['TOVr'] = plyr_sum['TO']/(plyr_sum['FGA']+0.44*plyr_sum['FTA']+plyr_sum['TO'])

    # adv stat: usage rate
    plyr_advanced['USGr'] = ((plyr_sum['FGA']+0.44*plyr_sum['FTA']+plyr_sum['TO'])*(TM_MP/5))/(plyr_sum['MIN']*(TM_FGA+0.44*TM_FTA+TM_TOV))

    # adv stat: offensive win share and ortg
    qAST = ((plyr_sum['MIN']/(TM_MP/5))*(1.14*((TM_AST-plyr_sum['AST'])/TM_FGM)))+((((TM_AST/TM_MP)*plyr_sum['MIN']*5-plyr_sum['AST'])/((TM_FGM/TM_MP)*plyr_sum['MIN']*5-plyr_sum['FGM']))*(1-(plyr_sum['MIN']/(TM_MP/5))))
    if plyr_sum['FGA'] == 0.0:
        FG_part = plyr_sum['FGM']*(1-0.5*(0.0)*qAST)
    else:
        FG_part = plyr_sum['FGM']*(1-0.5*((plyr_sum['PTS']-plyr_sum['FTM'])/(2*plyr_sum['FGA']))*qAST)
    AST_part = 0.5*(((TM_PTS-TM_FTM)-(plyr_sum['PTS']-plyr_sum['FTM']))/(2*(TM_FGA-plyr_sum['FGA'])))*plyr_sum['AST']
    if plyr_sum['FTA'] == 0.0:
        FT_part = (1-(1-(0.0))**2)*0.4*plyr_sum['FTA']
    else:
        FT_part = (1-(1-(plyr_sum['FTM']/plyr_sum['FTA']))**2)*0.4*plyr_sum['FTA']
    TM_scoring_poss = TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*TM_FTA*0.4
    TM_plays = TM_scoring_poss/(TM_FGA+TM_FTA*0.4+TM_TOV)
    TM_ORBr = TM_ORB/(TM_ORB+(OP_TRB-OP_ORB))
    TM_ORB_weight = ((1-TM_ORBr)*TM_plays)/((1-TM_ORBr)*TM_plays+TM_ORBr*(1-TM_plays))
    ORB_part = plyr_sum['OREB']*TM_ORB_weight*TM_plays
    missed_FG_pos = (plyr_sum['FGA']-plyr_sum['FGM'])*(1-1.07*TM_ORBr)
    if plyr_sum['FTA'] == 0.0:
        missed_FT_pos = ((1-(0.0))**2)*0.4*plyr_sum['FTA']
    else:
        missed_FT_pos = ((1-(plyr_sum['FTM']/plyr_sum['FTA']))**2)*0.4*plyr_sum['FTA']
    if plyr_sum['FGA'] == 0.0:
        PProd_FG_part = 2*(plyr_sum['FGM']+0.5*plyr_sum['FG3M'])*(1-0.5*(0.0)*qAST)
    else:
        PProd_FG_part = 2*(plyr_sum['FGM']+0.5*plyr_sum['FG3M'])*(1-0.5*((plyr_sum['PTS']-plyr_sum['FTM'])/(2*plyr_sum['FGA']))*qAST)

    PProd_AST_part = 2*((TM_FGM - plyr_sum['FGM'] + 0.5*(TM_3PM-plyr_sum['FG3M']))/(TM_FGM-plyr_sum['FGM']))*0.5*(((TM_PTS-TM_FTM)-(plyr_sum['PTS']-plyr_sum['FTM']))/(2*(TM_FGA-plyr_sum['FGA'])))*plyr_sum['AST']
    PProd_ORB_part = plyr_sum['OREB']*TM_ORB_weight*TM_plays*(TM_PTS/(TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*0.4*TM_FTA))
    points_produced = (PProd_FG_part+PProd_AST_part+plyr_sum['FTM'])*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+PProd_ORB_part
    scoring_posessions = (FG_part+AST_part+FT_part)*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+ORB_part
    total_off_possessions = scoring_posessions+missed_FG_pos+missed_FT_pos+plyr_sum['TO']
    if total_off_possessions == 0:
        plyr_advanced['ORtg'] = 0.0
    else:
        plyr_advanced['ORtg'] = 100*points_produced/total_off_possessions
    marginal_offense = points_produced-0.92*(league_stats['PPS'])*total_off_possessions
    marginal_pts_per_win = 0.32*(league_stats['PPG'])*((TM_PACE)/(league_stats['PACE']))

    plyr_advanced['OWS'] = marginal_offense/marginal_pts_per_win

    # adv stat: defensive win share and drtg
    DORr = OP_ORB / (OP_ORB + TM_DRB)
    DFGr = OP_FGM / OP_FGA
    TM_DRTG = 100*(OP_PTS/TM_POS)
    FMwt = (DFGr*(1-DORr))/(DFGr*(1-DORr)+(1-DFGr)*DORr)
    stops1 = plyr_sum['STL']+plyr_sum['BLK']*FMwt*(1-1.07*DORr)+plyr_sum['DREB']*(1-FMwt)
    stops2 = (((OP_FGA-OP_FGM-TM_BLK)/TM_MP)*FMwt*(1-1.07*DORr)+((OP_TOV-TM_STL)/TM_MP))*plyr_sum['MIN']+(plyr_sum['PF']/TM_PF)*0.4*OP_FTA*(1-(OP_FTM/OP_FTA))**2
    stops = stops1+stops2
    stopr = (stops*OP_MP)/(TM_POS*plyr_sum['MIN'])
    D_pts_per_ScPoss = OP_PTS/(OP_FGM+(1-(1-(OP_FTM/OP_FTA))**2)*OP_FTA*0.4)
    DRtg = TM_DRTG+0.2*(100*D_pts_per_ScPoss*(1-stopr)-TM_DRTG)
    plyr_advanced['DRtg'] = DRtg
    marginal_defense = (plyr_sum['MIN']/TM_MP)*(TM_POS)*(1.08*(league_stats['PPS'])-((DRtg)/100))
    plyr_advanced['DWS'] = marginal_defense/marginal_pts_per_win

    # adv stat: total win share
    plyr_advanced['WS'] = plyr_advanced['DWS'] + plyr_advanced['OWS']

    # adv stat: win share per 48 minutes
    plyr_advanced['WS48'] = plyr_advanced['WS'] / ((TM_MP/5)/plyr_sum['MIN'])

    return plyr_advanced


def calc_poss(team_sum, opp_sum):
    '''
    calc_poss: takes in summed team and opponent stats and returns a number of possessions
    '''
    TM_FGA, TM_FTA = team_sum['FGA'], team_sum['FTA']
    TM_ORB, TM_DRB = team_sum['OREB'], team_sum['DREB']
    TM_FGM, TM_TOV = team_sum['FGM'], team_sum['TO']

    OP_FGA, OP_FTA = opp_sum['FGA'], opp_sum['FTA']
    OP_ORB, OP_DRB = opp_sum['OREB'], opp_sum['DREB']
    OP_FGM, OP_TOV = opp_sum['FGM'], opp_sum['TO']

    possessions = 0.5 * ((TM_FGA + 0.4 * TM_FTA - 1.07 * (TM_ORB / (TM_ORB + OP_DRB)) * (TM_FGA - TM_FGM) + TM_TOV) + (OP_FGA + 0.4 * OP_FTA - 1.07 * (OP_ORB / (OP_ORB + TM_DRB)) * (OP_FGA - OP_FGM) + OP_TOV))

    return possessions


def calc_pace(team_sum, tm_pos, op_pos):
    '''
    calc_poss: takes in summed team and opponent stats and returns a pace factor
    '''
    TM_POS = tm_pos
    OP_POS = op_pos
    TM_MP = team_sum['MIN']
    pace = 48 * ((TM_POS + OP_POS) / (2 * (TM_MP / 5)))

    return pace


def create_player_lists(df_bs, df_teams):
    '''
    create_player_lists: Returns a dataframe with a column DND_IDS, which contains a list of player ids for inactive players.

    Inactive players are defined as players who were listed as DND (did not dress) or NWT (not with team)
    '''

    # take out nan dates, ie games that aren't in the OU data
    df_bs = df_bs[~np.isnan(df_bs['Date'])]
    # df_inactives = df_inactives[~np.isnan(df_inactives['Date'])]

    # index teams bs on team id and date for easier access
    df_teams = df_teams.set_index(['TEAM_ID', 'Date']).sort_index()

    # create dataframes with players who DND and NWT
    df_dnd = df_bs[df_bs['COMMENT'].str.contains('DND', na=False)]
    df_without_dnd = df_bs[~df_bs['COMMENT'].str.contains('DND', na=False)]
    df_nwt = df_without_dnd[df_without_dnd['COMMENT'].str.contains('NWT', na=False)]

    # initialize empty lists for each column
    df_teams['PLAYER_IDS'] = np.empty((len(df_teams), 0)).tolist()
    # df_teams['INACTIVE_IDS'] = np.empty((len(df_teams), 0)).tolist()
    df_teams['DND_IDS'] = np.empty((len(df_teams), 0)).tolist()

    # populate list with all player IDs in boxscore
    df_bs.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'PLAYER_IDS'].append(row['PLAYER_ID'])), axis=1)

    # populate list with all player IDs who DND
    df_dnd.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'DND_IDS'].append(row['PLAYER_ID'])), axis=1)

    # populate list with all player IDs who were NWT
    df_nwt.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'DND_IDS'].append(row['PLAYER_ID'])), axis=1)

    # populate list with all inactive players
    # df_inactives.apply((lambda row: df_teams.at[(row['TEAM_ID'], row['Date']), 'INACTIVE_IDS'].append(row['PLAYER_ID'])), axis=1)

    return df_teams

'''
Helper functions that create dictionaries from dataframes
'''


def create_player_shots_dict(row, player_shots_dict, colnames, player_id):
    '''
    create_player_shots_dict: Helper function to be used with df.apply() to create a dict containing player shots data.
    '''
    curr_date = row.name
    curr_dict = collections.OrderedDict(zip(colnames, row.values))
    player_shots_dict[player_id].update({curr_date: curr_dict})


def create_global_xefg_dict(row, global_xefg_dict, colnames):
    '''
    create_global_xefg_dict: Helper function to be used with df.apply() to create a dict containing global_xefg data previous to the current date.
    '''
    size = row.name[0]
    curr_date = row.name[1]

    curr_dict = collections.OrderedDict((zip([size + '_' + col for col in colnames], row.values)))
    global_xefg_dict[curr_date].update(curr_dict)


def create_team_shots_dict(row, team_shots_dict, colnames, team_id, size):
    '''
    create_team_shots_dict: Helper function to be used with df.apply() to create a dict containing shots data of team defense.
    '''
    game_id = row.pop('game_id')
    curr_dict = collections.OrderedDict((zip(colnames, row.values)))
    team_shots_dict[team_id][size].update({game_id: curr_dict})


'''
Functions to calculate major statistics, ie sum/avg/shots
'''


def calc_player_shots(df_bs, df_player_xefg):
    '''
    calc_player_shots: Returns the dicts player_shots_dates and player_shots_dict.

    player_shots_dates: A dict with keys=player_ids, values=list of dates a player had at least one shot on.

    player_shots_dict: A 2d dict with keys=player_ids and dates, values=pps and attempt for a given player over the last n games (history window) THROUGH the given date.

    Note: A list of player ids from the boxscore data (rather than the list of ids from the shots data) is used for the keys, to avoid out-of-bounds index selection later on.
    '''

    # create a list of desired columns in shots data
    colnames = [zone + '_pps' for zone in shot_zones]
    colnames = colnames + [zone + '_attempt' for zone in shot_zones]

    # create list of all unique player ids in bs data
    player_ids = pd.unique(df_bs.index.get_level_values('PLAYER_ID'))

    # create list of all unique players in shots data
    player_shots_ids = pd.unique(df_player_xefg.index.get_level_values('player_id'))

    # dict with key: value being player_id: all dates given player took shots
    player_shots_dates = {key: [] for key in player_ids}

    # dict with keys = player_id and date, values = data for last n games through that date
    player_shots_dict = {key: {} for key in player_ids}

    # traverse list of all players
    for player_id in player_shots_ids:

        # select rows in xefg data for current player
        df_curr_player = df_player_xefg.loc[player_id]

        # get all dates a given player played on
        player_shots_dates[player_id] = df_curr_player.index.values

        # calculate rolling sum over given history window for current player
        df_xefg_sum = pd.rolling_sum(df_curr_player, window=history_steps, min_periods=min_player_games)

        for zone in shot_zones:
            # calculate pps over summed window
            df_xefg_sum[zone + '_pps'] = df_xefg_sum[zone + '_pts'].div(df_xefg_sum[zone + '_attempt'])

        # reset PPS to 0 if attempts=0 (to adjust for division by zero)
        df_xefg_sum[(df_xefg_sum.isin({np.inf, -np.inf}))] = 0
        df_xefg_sum[np.isnan(df_xefg_sum)] = 0

        # populate player shots dict with calculated data
        df_xefg_sum[colnames].apply((lambda row: create_player_shots_dict(row, player_shots_dict, colnames, player_id)), axis=1)

    return player_shots_dates, player_shots_dict


def calc_global_xefg(df_global_xefg):
    '''
    calc_global_xefg: Returns a dict with xefg data from league-wide stats BEFORE a given date. Keys=dates, values=dict containing xefg data.
    '''
    # drop unwanted columns
    df_global_xefg = df_global_xefg.drop('Season', axis=1)

    # select out rows with desired player sizes
    df_global_xefg = df_global_xefg.loc[player_sizes]

    # create list with all dates
    dates_list = df_global_xefg.index.get_level_values('Date')

    # dict, keys = dates, values = xefg data prior to given date
    global_xefg_dict = {key: {} for key in dates_list}

    # get column names to use for dict creation
    colnames = df_global_xefg.columns.values.tolist()

    # generate global shots dict
    df_global_xefg.apply(lambda row: (create_global_xefg_dict(row, global_xefg_dict, colnames)), axis=1)

    return global_xefg_dict


def calc_team_shots(df_team_xefg):
    '''
    calc_team_shots: Returns the dict team_shots_dict, which is meant to reflect team shot DEFENSE by zone.

    team_shots_dict: 2d dict with keys=team_ids and game_ids. Values=PPS/attempt defensive data for a given team over the pre-set history window for the games BEFORE the given game id. Note that this is DIFFERENT from how player_shots_dict and player_bs_dict are keyed.
    '''
    # list of all team ids for current season
    team_ids = pd.unique(df_team_xefg.index.get_level_values('team_id'))

    # dict with keys = team_id, player_size, and game_id. values=defensive shots data for given game and team
    team_shots_dict = {key: {size: {} for size in player_sizes} for key in team_ids}

    for team_id in team_ids:  # iterate through the teams

        # select defensive stats for current team
        df_curr_team_xefg = df_team_xefg.loc['defense', team_id]

        for size in player_sizes:  # iterate through the different player sizes

            # calculate rolling sum over given history window
            df_team_xefg_sum = pd.rolling_sum(df_curr_team_xefg.loc[size], window=history_steps, min_periods=history_steps)

            for zone in shot_zones:
                # calculate pps over summed window
                df_team_xefg_sum[zone + '_pps'] = df_team_xefg_sum[zone + '_pts'].div(df_team_xefg_sum[zone + '_attempt'])

            # reset PPS to 0 if attempts=0 (to adjust for division by zero)
            df_team_xefg_sum[(df_team_xefg_sum.isin({np.inf, -np.inf}))] = 0
            df_team_xefg_sum[np.isnan(df_team_xefg_sum)] = 0

            # shift game ids by 1 spot, so that data reflects summed games prior to current game
            df_team_xefg_sum['game_id'] = df_curr_team_xefg.loc[size].game_id
            df_team_xefg_sum['game_id'] = df_team_xefg_sum.shift(-1)

            # get list of labels for desired columns
            colnames = [zone + '_pps' for zone in shot_zones]
            colnames = colnames + [zone + '_attempt' for zone in shot_zones]
            colnames.append('game_id')

            # add calculated sum data to team_shots_dict
            df_team_xefg_sum[colnames].apply((lambda row: create_team_shots_dict(row, team_shots_dict, colnames, team_id, size)), axis=1)

    return team_shots_dict


def calc_shots_stats(df_bs, df_player_xefg, df_team_xefg, df_global_xefg):
    player_shots_dates, player_shots_dict = calc_player_shots(df_bs, df_player_xefg)
    return player_shots_dates, player_shots_dict, calc_team_shots(df_team_xefg), calc_global_xefg(df_global_xefg)


def prev_date_index(player_dates, curr_date):
    '''
    prev_date_index: Helper function. Searches a list (nparray) of dates for a given player. Returns the index along the array of the LAST date a player played or shot on, before the current date.
    '''
    return np.searchsorted(player_dates, curr_date) - 1


'''
Functions that collect variables/features into list for outputting
'''


def form_team_vars(home_team_id, away_team_id, game_id, team_shots_dict):
    team_vars = []
    team_vars_header = []
    for size in player_sizes:
        home_team_vars = team_shots_dict[home_team_id][size][game_id]
        away_team_vars = team_shots_dict[away_team_id][size][game_id]

        team_vars.extend(home_team_vars.values())
        team_vars_header.extend(['home_' + str(size) + '_' + str(key) for key in home_team_vars.keys()])
        team_vars.extend(away_team_vars.values())
        team_vars_header.extend(['away_' + str(size) + '_' + str(key) for key in away_team_vars.keys()])

    return team_vars, team_vars_header


def form_global_vars(date, global_xefg_dict):
    global_vars = []
    global_vars_header = []
    global_vars = global_xefg_dict[date].values()
    global_vars_header = ['global_' + str(key) for key in global_xefg_dict[game_date].keys()]

    return global_vars, global_vars_header


def form_player_vars(home_output, away_output):
    player_vars = []
    player_vars_header = []
    for index, player_dict in enumerate(home_output):
        player_vars.extend(player_dict.values())
        player_vars_header.extend(['home_p' + str(index + 1) + '_' + str(key) for key in player_dict.keys()])

    for index, player_dict in enumerate(away_output):
        final_row.extend(player_dict.values())
        player_vars_header.extend(['away_p' + str(index + 1) + '_' + str(key) for key in player_dict.keys()])

    return player_vars, player_vars_header


def form_game_vars(row, game_id):
    home_rest, away_rest = get_rest(row)
    y, push, line = get_result(row)
    home_team_id, away_team_id = get_team_ids(row)
    game_vars = [home_rest, away_rest, line, push, y, game_id, home_team_id, away_team_id]
    game_vars_header = ['home_REST', 'away_REST', 'LINE', 'PUSH', 'y', 'game_id', 'home_team', 'away_team']

    return game_vars, game_vars_header


def form_final_row(player_vars, team_vars, global_vars, game_vars, final_row):
    final_row.extend(player_vars + team_vars + global_vars + game_vars)
    return final_row


def form_final_header(player_vars_header, team_vars_header, global_vars_header, game_vars_header, final_row_header):
    final_row_header.extend(player_vars_header + team_vars_header + global_vars_header + game_vars_header + final_row_header)
    return final_row_header


def iterate_player_list(player_list, inactive_list, player_shots_dates, player_shots_dict, date, team_id, game_id, player_bs_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum, league_stats, home):
    '''
    iterate_player_list: calculates all stats for players in a player list
    Returns an array of all stats, or an empty array if any player in the player list had zero previous games played
    '''
    # init empty output lists
    roster_output = []
    error_output = []

    for player_id in player_list:  # iterate over players
        # get previous games by player
        # player_dates, player_teams, player_opps, df_players = player_prev_games(history_steps, game_id, player_id, df_bs)

        last_played_index = prev_date_index(player_bs_dates[player_id], date)
        last_shot_index = prev_date_index(player_shots_dates[player_id], date)

        # if no previous games for a player, quit loop and return error
        if (last_played_index + 1) < min_player_games or (last_shot_index + 1) < min_player_games:
            return error_output

        last_played_date = player_bs_dates[player_id][last_played_index]
        last_shot_date = player_shots_dates[player_id][last_shot_index]

        # sum and average player stats
        plyr_sum, plyr_avg = bs_sum[player_id][last_played_date], bs_avg[player_id][last_played_date]

        # adjust plus_minus stats and percentage stats
        plyr_avg['PLUS_MINUS'] = plyr_sum['PLUS_MINUS']
        plyr_avg['FG_PCT'] = 0.0 if (plyr_sum['FGA'] == 0) else (plyr_sum['FGM'] / plyr_sum['FGA'])
        plyr_avg['FG3_PCT'] = 0.0 if (plyr_sum['FG3A'] == 0) else (plyr_sum['FG3M'] / plyr_sum['FG3A'])
        plyr_avg['FT_PCT'] = 0.0 if (plyr_sum['FTA'] == 0) else (plyr_sum['FTM'] / plyr_sum['FTA'])

        # include player shots data
        player_shots_data = player_shots_dict[player_id][last_shot_date]

        team_sum, opp_sum = bs_team_sum[player_id][last_played_date], bs_opp_sum[player_id][last_played_date]

        # calculate advanced stats
        plyr_advanced = calc_advanced_stats(plyr_sum, team_sum, opp_sum, player_id, league_stats)

        # mark if active or inactive for current game
        plyr_avg['ACTIVE'] = 0 if player_id in inactive_list else 1

        # mark if home or away player
        plyr_avg['HOME_TEAM'] = 1 if home else 0

        # combine dicts
        plyr_avg.update(plyr_advanced)
        plyr_avg.update(player_shots_data)

        # create ordered dict to maintain output order
        od = collections.OrderedDict(sorted(plyr_avg.items()))
        # add data to final output
        roster_output.append(od)

    # sort by minutes played
    roster_output = sorted(roster_output, key=lambda k: k['MIN'], reverse=True)

    # take the first n players and return value
    return roster_output[:num_players]


if __name__ == "__main__":
    start_time = time.time()  # timer function

    print 'Loading shots dataframes...'
    df_lines_all, df_player_xefg_all, df_team_xefg_all, df_global_xefg_all = load_dataframes()

    # sorting dataframes by date
    df_player_xefg_all.sort_index(inplace=True)
    df_team_xefg_all.sort_index(inplace=True)
    df_global_xefg_all.sort_index(inplace=True)

    final_data = []

    for year in range(min_year, max_year + 1):  # iterate over seasons
        print 'Now processing season 20' + "%02d" % (year,)

        # select only current season of all dataframes
        print 'Selecting current season of shots dataframes'
        df_lines = slice_df(df_lines_all, year, 'boolean')
        df_player_xefg = slice_df(df_player_xefg_all, year, 'query')
        df_team_xefg = slice_df(df_team_xefg_all, year, 'index')
        df_global_xefg = slice_df(df_global_xefg_all, year, 'boolean')

        # load data into dataframes
        print 'Loading boxscore dataframes'
        df_bs, df_teams = load_boxscores(year)

        # create dataframe with active/inactive player lists for each game
        print 'Creating inactive player lists'
        df_player_lists = create_player_lists(df_bs, df_teams)

        # set indexes on dataframe for easier searching
        df_bs.set_index(['PLAYER_ID', 'Date'], inplace=True)
        df_bs.sort_index(inplace=True)
        df_teams.set_index(['TEAM_ID', 'Date'], inplace=True)
        df_teams.sort_index(inplace=True)

        # create dicts of sum and average stats
        print 'Creating player sum and average dicts'
        player_bs_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum = calc_sum_avg_stats(df_bs, df_teams)

        print 'Creating player, team, global shots dicts'
        player_shots_dates, player_shots_dict, team_shots_dict, global_xefg_dict = calc_shots_stats(df_bs, df_player_xefg, df_team_xefg, df_global_xefg)

        print 'Now iterating through game list'

        for index, row in df_lines.iterrows():  # iterate over games
            game_id = index
            game_date = row['Date']

            # skip if playoff game
            if np.isnan(index):
                continue

            # get team ids
            home_team_id, away_team_id = get_team_ids(row)

            # filter dataframes to games previous to current date to improve performance
            df_teams_prev = df_teams[df_teams.index.get_level_values('Date') < game_date]

            # check that sufficient number of games played
            team_games = min(len(df_teams_prev.loc[home_team_id].index), len(df_teams_prev.loc[away_team_id].index))

            if (team_games < history_steps):
                continue

            # calculate league stats
            league_stats = calc_league_stats(game_id, df_teams_prev)

            # get player lists from current game boxscore
            home_player_list, away_player_list = df_player_lists.at[(home_team_id, game_date), 'PLAYER_IDS'], df_player_lists.at[(away_team_id, game_date), 'PLAYER_IDS']

            # get DND/NWT list from current boxscore
            home_inactive_list, away_inactive_list = df_player_lists.at[(home_team_id, game_date), 'DND_IDS'], df_player_lists.at[(away_team_id, game_date), 'DND_IDS']

            # iterate over player lists and calculate player stats
            home_output = iterate_player_list(home_player_list, home_inactive_list, player_shots_dates, player_shots_dict, game_date, home_team_id, game_id, player_bs_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum, league_stats, True)

            # skip if any players don't have a previous game
            if not home_output:
                continue

            away_output = iterate_player_list(away_player_list, away_inactive_list, player_shots_dates, player_shots_dict, game_date, away_team_id, game_id, player_bs_dates, bs_sum, bs_avg, bs_team_sum, bs_opp_sum, league_stats, False)

            # skip if any players don't have a previous game
            if not away_output:
                continue

            # create a row of data and append it to our final output
            final_row = []

            # aggregating player, team, global, and game features
            player_vars, player_vars_header = form_player_vars(home_output, away_output)

            team_vars, team_vars_header = form_team_vars(home_team_id, away_team_id, game_id, team_shots_dict)

            global_vars, global_vars_header = form_global_vars(game_date, global_xefg_dict)

            game_vars, game_vars_header = form_game_vars(row, game_id)

            # adding all features to our final row
            form_final_row(player_vars, team_vars, global_vars, game_vars, final_row)

            # append our final row to our header
            final_data.append(final_row)

            # print 'GAME ID: {}\r'.format(game_id),
            # sys.stdout.flush()

        if year == 11:
            temp_header = []
            form_final_header(player_vars_header, team_vars_header, global_vars_header, game_vars_header, temp_header)

        print "Processing complete for season 20" + "%02d" % (year,)

    # create our list of column names
    final_header = []
    form_final_header(player_vars_header, team_vars_header, global_vars_header, game_vars_header, final_header)

    # print list(set(final_header) - set(temp_header))
    # print list(set(temp_header) - set(final_header))

    # create a pandas dataframe containing all our data
    df_final_data = pd.DataFrame.from_records(final_data, columns=final_header)

    print df_final_data.shape
    df_final_data.to_pickle('data.p')

    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
