import pandas as pd
import numpy as np
import tables
import math
import time

min_year, max_year = 3, 3  # years to iterate over ie 3, 14 means 2003-2014
history_steps = 5  # num of games back to use for stats
min_player_games = 1  # num of games each player has to play at minimum
num_players = 9  # number of players to use from roster

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


def dict_column_avg(df_avg):
    '''
    dict_column_avg: helper function that averages the columns in a df and returns to a dict

    Note: Significantly faster than pd.mean(axis=0), because the pandas mean function has performance issues when used on non-float datatypes
    '''
    stats_header = df_avg.columns.values.tolist()
    dict_avg = dict(zip(stats_header, [df_avg[col].values.mean(axis=0) for col in stats_header]))

    print dict_avg
    return dict_avg


def dict_avg(plyr_sum, num_games):
    plyr_avg = {}
    for key, value in plyr_sum.items():
        plyr_avg[key] = value / num_games

    plyr_avg['PLUS_MINUS'] = plyr_sum['PLUS_MINUS']

    # adjust percentage stats to reflect actual percentages
    plyr_avg['FG_PCT'] = 0.0 if (plyr_sum['FGA'] == 0) else (plyr_sum['FGM'] / plyr_sum['FGA'])
    plyr_avg['FG3_PCT'] = 0.0 if (plyr_sum['FG3A'] == 0) else (plyr_sum['FG3M'] / plyr_sum['FG3A'])
    plyr_avg['FT_PCT'] = 0.0 if (plyr_sum['FTA'] == 0) else (plyr_sum['FTM'] / plyr_sum['FTA'])

    return plyr_avg


def minutes(td):
    '''
    minutes: helper function that converts a timedelta object to minutes
    '''
    return td / np.timedelta64(1, 'm')

'''
Helper functions to construct strings
'''


def game_string(game_list):
    '''
    game_string: Helper function to make strings for pd.query()
    Note: Stopped using pd.query() for the most part for performance reasons
    '''
    return '[' + ', '.join(map(str, game_list)) + ']'


def query(conditions, values):
    '''
    query: helper function to help make a query string to feed into pd.query()
    Note: Largely stopped using pd.query() for performance reasons
    '''
    length = len(conditions)

    if len(conditions) != len(values):
        return -1

    queries = []
    for b in range(0, length):
        queries.append('(' + str(conditions[b]) + str(values[b]) + ')')

        query_string = ' & '.join(queries)

    return query_string


'''
Functions that get data from the lines dataframe
'''


def get_result(row):
    '''
    get_result: gets the line and ATS result from the lines df
    '''
    y = 1 if row['ATSr'] == 'W' else 0

    line = float(row['Line'])

    return y, line


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
Functions that get lists of previous games
'''


def player_prev_games(n, game_id, player_id, df_bs):
    '''
    player_prev_games: returns the n prev games a player played in
    returns a list of game ids, a list of team ids the player played on in the last n games, and a dataframe of player boxscore data for those games
    '''

    # filter dataframe for games by player
    df_games = df_bs[(df_bs['PLAYER_ID'] == player_id)]

    # select last n games
    df_games = df_games.tail(n)

    # construct list of game ids and list of team ids a player played for
    game_list = df_games['GAME_ID'].values
    player_teams = list(pd.unique(df_games['TEAM_ID'].values))

    return game_list, player_teams, df_games


def team_prev_games(n, game_id, team_id, df_teams):
    '''
    team_prev_games: returns a list of game ids of the last n games a team played in
    '''
    # query previous games df for teams with same team id
    df_games = df_teams[(df_teams['TEAM_ID'] == team_id)]

    # select last n games
    df_games = df_games.tail(n)

    # create list of game ids
    game_list = df_games['GAME_ID'].values

    return game_list


def query_prev_games(df_bs, df_teams, game_id):
    '''
    query_prev_games: takes a dataframe of boxscore data for a season and returns a smaller dataframe of all previous games
    '''
    # query dataframes for game previous to current game id
    df_bs_prev = df_bs[df_bs['GAME_ID'] < game_id]
    df_teams_prev = df_teams[df_teams['GAME_ID'] < game_id]

    return df_bs_prev, df_teams_prev

'''
Functions that load dataframes and get player lists to calculate stats for
Note: python logic is faster than pd.query() for small dataframes
'''


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

    # read in lines data and limit to current season
    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]

    # load team and player boxscores
    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')

    return df_lines, df_bs, df_teams


def populate_rosters(game_id, home_team_id, away_team_id, df_teams, df_bs):
    '''
    populate_rosters: returns a home and away player list based on the boxscore of the previous game that each team played
    '''

    # get game id of previous home and away games
    home_prev_game = team_prev_games(1, game_id, home_team_id, df_teams)
    away_prev_game = team_prev_games(1, game_id, away_team_id, df_teams)

    # construct query strings
    cond = ['GAME_ID == ', 'TEAM_ID == ']
    home_query = query(cond, [home_prev_game, home_team_id])
    away_query = query(cond, [away_prev_game, away_team_id])

    # print home_prev_game, home_team_id
    # construct lists of player id values
    df_home = df_bs[(df_bs['GAME_ID'].isin(home_prev_game)) & (df_bs['TEAM_ID'] == home_team_id)]
    df_away = df_bs[(df_bs['GAME_ID'].isin(away_prev_game)) & (df_bs['TEAM_ID'] == away_team_id)]
    home_player_list = df_home['PLAYER_ID'].values
    away_player_list = df_away['PLAYER_ID'].values
    # home_player_list = df_bs.query(home_query).loc[:, 'PLAYER_ID'].values
    # away_player_list = df_bs.query(away_query).loc[:, 'PLAYER_ID'].values

    return home_player_list[:num_players], away_player_list[:num_players]


'''
Functions to calculate stats
'''


def calc_basic_stats(player_id, df_games):
    '''
    calc_basic_stats: calculates sum and average of boxscore stats for a given player over a given dataframe. Returns 2 dicts
    '''

    # select out unwanted columns
    df_prune = df_games.iloc[:, 8:]

    # sum and average columsn and write to dict
    plyr_sum = df_prune.sum(axis=0).to_dict()

    plyr_avg = dict_avg(plyr_sum, len(df_prune.index))  # using custom method for performance


    # add player id to sum dict
    plyr_sum['PLAYER_ID'] = player_id

    return plyr_sum, plyr_avg


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
    league_stats['PACE'] = 48 * ((league_possessions * 2) / (2 * (minutes(league_sum['MIN']) / 5)))

    league_stats['PPS'] = league_sum['PTS'] / (league_sum['FGA'] + 0.44 * league_sum['FTA'])

    return league_stats


def calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats):
    '''
    calc_advanced_stats: calculates all advanced stats based on the sum stats of a player, player's teams, and player's opponents. Returns a dict
    '''
    # initialize vars for easier formulas, cast to float for division
    TM_MP, OP_MP = minutes(team_sum['MIN']), minutes(opp_sum['MIN'])

    TM_FGA, TM_FTA = float(team_sum['FGA']), float(team_sum['FTA'])
    TM_FGM, TM_FTM = float(team_sum['FGM']), float(team_sum['FTM'])
    TM_3PA, TM_3PM = float(team_sum['FG3A']), float(team_sum['FG3M'])
    TM_ORB, TM_DRB = float(team_sum['OREB']), float(team_sum['DREB'])
    TM_FGM, TM_TOV = float(team_sum['FGM']), float(team_sum['TO'])
    TM_AST, TM_BLK = team_sum['AST'], team_sum['BLK']
    TM_STL, TM_PF = team_sum['STL'], team_sum['PF']
    TM_PTS = team_sum['PTS']
    TM_PACE = calc_pace(team_sum, opp_sum)
    TM_POS = calc_poss(team_sum, opp_sum)

    OP_FGA, OP_FTA = float(opp_sum['FGA']), float(opp_sum['FTA'])
    OP_FGM, OP_FTM = float(opp_sum['FGM']), float(opp_sum['FTM'])
    OP_3PA, OP_3PM = float(opp_sum['FG3A']), float(opp_sum['FG3M'])
    OP_ORB, OP_DRB = float(opp_sum['OREB']), float(opp_sum['DREB'])
    OP_FGM, OP_TOV = float(opp_sum['FGM']), float(opp_sum['TO'])
    OP_AST, OP_BLK = opp_sum['AST'], opp_sum['BLK']
    OP_STL, OP_PF = opp_sum['STL'], opp_sum['PF']
    OP_PTS = team_sum['PTS']
    OP_PACE = calc_pace(opp_sum, team_sum)
    OP_POS = calc_poss(opp_sum, team_sum)

    # initialize output dictionary with player id
    plyr_advanced = {'PLAYER_ID': plyr_sum['PLAYER_ID']}

    # convert minutes from Timedelta to minutes format
    plyr_sum['MIN'] = minutes(plyr_sum['MIN'])

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

    PProd_AST_part = 2*((TM_FGM - plyr_sum['FGM'] +0.5*(TM_3PM-plyr_sum['FG3M']))/(TM_FGM-plyr_sum['FGM']))*0.5*(((TM_PTS-TM_FTM)-(plyr_sum['PTS']-plyr_sum['FTM']))/(2*(TM_FGA-plyr_sum['FGA'])))*plyr_sum['AST']
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


def calc_team_opp_stats(game_list, player_teams, df_teams):
    '''
    calc_team_stats: takes in a list of games, a list of teams that a player played on, and returns summed team stats over a given number of teams
    '''
    df_games = df_teams[df_teams['GAME_ID'].isin(game_list)]
    df_team_stats = df_games[df_games['TEAM_ID'].isin(player_teams)]
    df_opp_stats = df_games[~df_games['TEAM_ID'].isin(player_teams)]

    # df_prune = df_games.iloc[:, 5:].drop(['FG_PCT', 'FG3_PCT', 'FT_PCT'], axis=1)

    team_sum = df_team_stats.sum(axis=0).to_dict()
    opp_sum = df_opp_stats.sum(axis=0).to_dict()
    # team_sum = dict_column_sum(df_prune)

    return team_sum, opp_sum


def calc_opp_stats(game_list, player_teams, df_teams):
    '''
    calc_opp_stats: takes in a list of games, a list of teams that a player played on, and returns summed opponent stats over a given number of teams
    '''
    # query games that a player played in but wasn't their team id
    df_games = df_teams[df_teams['GAME_ID'].isin(game_list) & ~df_teams['TEAM_ID'].isin(player_teams)]

    # df_prune = df_games.iloc[:, 5:].drop(['FG_PCT', 'FG3_PCT', 'FT_PCT'], axis=1)

    # opp_sum = df_prune.sum(axis=0).to_dict()
    opp_sum = df_games.sum(axis=0).to_dict()
    # opp_sum = dict_column_sum(df_prune)

    return opp_sum


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


def calc_pace(team_sum, opp_sum):
    '''
    calc_poss: takes in summed team and opponent stats and returns a pace factor
    '''
    TM_POS = calc_poss(team_sum, opp_sum)
    OP_POS = calc_poss(opp_sum, team_sum)
    TM_MP = minutes(team_sum['MIN'])
    pace = 48 * ((TM_POS + OP_POS) / (2 * (TM_MP / 5)))

    return pace


def history_met(game_id, team_games):
    '''
    history_met: supposedly checks if history requirements met but haven't been using this correctly
    '''
    if (team_games < history_steps):  # check that team has played enough games
        return False

    return True


def iterate_player_list(player_list, team_id, game_id, df_bs, df_teams, league_stats):
    '''
    iterate_player_list: calculates all stats for players in a player list
    Returns an array of all stats, or an empty array if any player in the player list had zero previous games played
    '''
    # init empty output lists
    roster_output = []
    error_output = []

    # temporary dicts to store data to avoid repeating queries
    team_histories = {}
    opp_histories = {}

    for player_id in player_list: # iterate over players
        # get previous games by player
        player_game_list, player_teams, df_players = player_prev_games(history_steps, game_id, player_id, df_bs)

        # if no previous games for a player, quit loop and return error
        if not player_game_list.size:
            return error_output

        # sum and average player stats
        plyr_sum, plyr_avg = calc_basic_stats(player_id, df_players)

        # form a string to act as dict key for team histories
        games_teams_key = np.array_str(np.append(player_game_list, player_teams))

        # check if team and opp sum have already been calculated
        if games_teams_key in team_histories:  # if already calculated
            # set team and opp sum to previously calculated stats
            team_sum, opp_sum = team_histories[games_teams_key], opp_histories[games_teams_key]
        else:  # if not already calculated
            # calculate team and opponent stats
            team_sum, opp_sum = calc_team_opp_stats(player_game_list, player_teams, df_teams)
            team_histories[games_teams_key] = team_sum
            opp_histories[games_teams_key] = opp_sum
        # opp_sum = calc_opp_stats(player_game_list, player_teams, df_teams)

        # calculate advanced stats
        plyr_advanced = calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats)

        # add data to final output
        roster_output.append(plyr_avg.values())
        roster_output.append(plyr_advanced.values())

    return roster_output

if __name__ == "__main__":
    start_time = time.time()  # timer function

    for year in range(min_year, max_year + 1):  # iterate over seasons

        # load data into dataframes
        df_lines, df_bs, df_teams = load_dataframes(year)

        for index, row in df_lines.iterrows(): # iterate over games
            game_id = index

            # skip if playoff game
            if np.isnan(index):
                continue

            # get team ids
            home_team_id, away_team_id = get_team_ids(row)

            # filter dataframes to games previous to current one to improve performance
            df_bs_prev, df_teams_prev = query_prev_games(df_bs, df_teams, game_id)

            # check that sufficient number of games played
            team_games = min(len(team_prev_games(history_steps, game_id, home_team_id, df_teams_prev)), len(team_prev_games(history_steps, game_id, away_team_id, df_teams_prev)))

            if (team_games < history_steps):
                continue

            # get player lists from previous boxscore
            home_player_list, away_player_list = populate_rosters(game_id, home_team_id, away_team_id, df_teams_prev, df_bs_prev)

            # get line and result data
            y, line = get_result(row)
            home_rest, away_rest = get_rest(row)

            # calculate league stats
            league_stats = calc_league_stats(game_id, df_teams_prev)

            # filter boxscore df for min > 0 to improve query speed for stats calculations
            df_bs_played = df_bs_prev[(df_bs_prev['MIN'] > np.timedelta64(0))]

            # iterate over player lists and calculate player stats
            home_output = iterate_player_list(home_player_list, home_team_id, game_id, df_bs_played, df_teams_prev, league_stats)

            # skip if any players don't have a previous game
            if not home_output:
                continue

            away_output = iterate_player_list(away_player_list, away_team_id, game_id, df_bs_played, df_teams_prev, league_stats)

            # skip if any players don't have a previous game
            if not away_output:
                continue

            print str(int(game_id))

    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
