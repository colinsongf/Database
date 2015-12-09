import pandas as pd
import numpy as np
import tables
import math
import time

min_year, max_year = 3, 3
history_steps = 5
min_player_games = 1
num_players = 9  # number of players to use from roster


def load_dataframes(year):
    year_indicator = "%02d" % (year,)
    season_cond = '20' + year_indicator
    lines_filepath = './data/lines/lines.h5'
    players_filepath = './data/players/bs' + year_indicator + '.h5'
    team_filepath = './data/teams/tbs' + year_indicator + '.h5'

    df_lines = pd.read_hdf(lines_filepath, 'df_lines')
    df_lines = df_lines[df_lines['Season'] == int(season_cond)]

    df_bs = pd.read_hdf(players_filepath, 'df_bs')
    df_teams = pd.read_hdf(team_filepath, 'df_team_bs')

    return df_lines, df_bs, df_teams


def player_prev_games(n, game_id, player_id, df_bs):
    df_games = df_bs[(df_bs['PLAYER_ID'] == player_id) & (df_bs['GAME_ID'] < game_id)]

    # drop games with 0 minutes played and select last n games
    df_games = df_games[df_games['MIN'] > np.timedelta64(0)].tail(n)

    game_list = df_games.loc[:, 'GAME_ID'].values
    player_teams = list(set(df_games.loc[:, 'TEAM_ID'].values))
    return game_list, player_teams, df_games


def team_prev_games(n, game_id, team_id, df_teams):
    df_games = df_teams[(df_teams['TEAM_ID'] == team_id) & (df_teams['GAME_ID'] < game_id)]
    df_games = df_games.tail(n)

    game_list = df_games.loc[:, 'GAME_ID'].values

    return game_list


def get_result(row):
    y = 1 if row['ATSr'] == 'W' else 0

    line = float(row['Line'])

    return y, line


def get_rest(row):
    rest_array = row['Rest'].split('&')
    home_rest = 0 if rest_array[0] == '' else rest_array[0]
    away_rest = 0 if rest_array[1] == '' else rest_array[1]

    return home_rest, away_rest


def get_team_ids(row):
    home_team = row['home_id']
    away_team = row['away_id']

    return home_team, away_team



'''
def calculate_player_stats(df_games, player_id):
    # calculate player stats from game

def calculate_team_stats(df_team_bs, game_list, team_id):
    # calculates sum and average stats for team over a given game list

def calculate_opp_stats(df_team_bs, game_list, team_id):
# calculates sum and average stats for opposing teams over a given game list

def calculate_advanced_stats(player_stats, team_stats, opp_stats)


'''


def query(conditions, values):
    length = len(conditions)

    if len(conditions) != len(values):
        return -1

    queries = []
    for b in range(0, length):
        queries.append('(' + str(conditions[b]) + str(values[b]) + ')')

        query_string = ' & '.join(queries)

    return query_string


def populate_rosters(game_id, home_team_id, away_team_id, df_teams, df_bs):

    home_prev_game = team_prev_games(1, game_id, home_team_id, df_teams)
    away_prev_game = team_prev_games(1, game_id, away_team_id, df_teams)

    cond = ['GAME_ID == ', 'TEAM_ID == ']
    home_query = query(cond, [home_prev_game, home_team_id])
    away_query = query(cond, [away_prev_game, away_team_id])

    # print df_bs.query(home_query)
    home_player_list = df_bs.query(home_query).loc[:, 'PLAYER_ID'].values
    away_player_list = df_bs.query(away_query).loc[:, 'PLAYER_ID'].values

    return home_player_list[:num_players], away_player_list[:num_players]


def calc_basic_stats(player_id, df_games):
    df_prune = df_games.iloc[:, 8:]
    df_sum = df_prune.sum(axis=0)

    if pd.isnull(df_prune).any():
        print 'NaN'
    plyr_sum = df_sum.to_dict()

    df_avg = df_prune.mean(axis=0)
    plyr_avg = df_avg.to_dict()

    plyr_avg['FG_PCT'] = 0.0 if (plyr_sum['FGA'] == 0) else (plyr_sum['FGM'] / plyr_sum['FGA'])
    plyr_avg['FG3_PCT'] = 0.0 if (plyr_sum['FG3A'] == 0) else (plyr_sum['FG3M'] / plyr_sum['FG3A'])
    plyr_avg['FT_PCT'] = 0.0 if (plyr_sum['FTA'] == 0) else (plyr_sum['FTM'] / plyr_sum['FTA'])
    plyr_sum['PLAYER_ID'] = player_id

    return plyr_sum, plyr_avg


def calc_league_stats(game_id, df_teams):
    query_string = '(GAME_ID < ' + str(game_id) + ')'
    df_league_games = df_teams.query(query_string)
    df_league_sum = df_league_games.iloc[:, 5:].sum(axis=0)

    league_stats = {}
    TM_FGA, TM_FTA = df_league_sum['FGA'], df_league_sum['FTA']
    TM_FGM, TM_TOV = df_league_sum['FGM'], df_league_sum['TO']
    TM_ORB, TM_DRB = df_league_sum['OREB'], df_league_sum['DREB']
    OP_FGA, OP_FTA, OP_FGM, OP_ORB, OP_DRB, OP_TOV = TM_FGA, TM_FTA, TM_FGM, TM_ORB, TM_DRB, TM_TOV

    league_possessions = 0.5 * ((TM_FGA + 0.4 * TM_FTA - 1.07 * (TM_ORB / (TM_ORB + OP_DRB)) * (TM_FGA - TM_FGM) + TM_TOV) + (OP_FGA + 0.4 * OP_FTA - 1.07 * (OP_ORB / (OP_ORB + TM_DRB)) * (OP_FGA - OP_FGM) + OP_TOV))

    num_games = len(df_league_games.index) / 2
    league_stats['PPG'] = df_league_sum['PTS'] / num_games
    league_stats['PACE'] = 48 * ((league_possessions * 2) / (2 * (minutes(df_league_sum['MIN']) / 5)))

    league_stats['PPS'] = df_league_sum['PTS'] / (df_league_sum['FGA'] + 0.44 * df_league_sum['FTA'])

    return league_stats

def calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats):

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

    plyr_advanced = {'PLAYER_ID': plyr_sum['PLAYER_ID']}

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


def game_string(game_list):
    return '[' + ', '.join(map(str, game_list)) + ']'


def calc_team_stats(game_list, player_teams, df_teams):
    df_games = df_teams[df_teams['GAME_ID'].isin(game_list) & df_teams['TEAM_ID'].isin(player_teams)]

    df_prune = df_games.iloc[:, 5:].drop(['FG_PCT', 'FG3_PCT', 'FT_PCT'], axis=1)
    df_team_sum = df_prune.sum(axis=0)

    return df_team_sum.to_dict()


def calc_opp_stats(game_list, player_teams, df_teams):
    df_games = df_teams[df_teams['GAME_ID'].isin(game_list) & ~df_teams['TEAM_ID'].isin(player_teams)]

    df_prune = df_games.iloc[:, 5:].drop(['FG_PCT', 'FG3_PCT', 'FT_PCT'], axis=1)
    df_opp_sum = df_prune.sum(axis=0)

    return df_opp_sum.to_dict()


def calc_poss(team_sum, opp_sum):
    TM_FGA, TM_FTA = team_sum['FGA'], team_sum['FTA']
    TM_ORB, TM_DRB = team_sum['OREB'], team_sum['DREB']
    TM_FGM, TM_TOV = team_sum['FGM'], team_sum['TO']

    OP_FGA, OP_FTA = opp_sum['FGA'], opp_sum['FTA']
    OP_ORB, OP_DRB = opp_sum['OREB'], opp_sum['DREB']
    OP_FGM, OP_TOV = opp_sum['FGM'], opp_sum['TO']

    possessions = 0.5 * ((TM_FGA + 0.4 * TM_FTA - 1.07 * (TM_ORB / (TM_ORB + OP_DRB)) * (TM_FGA - TM_FGM) + TM_TOV) + (OP_FGA + 0.4 * OP_FTA - 1.07 * (OP_ORB / (OP_ORB + TM_DRB)) * (OP_FGA - OP_FGM) + OP_TOV))

    return possessions


def calc_pace(team_sum, opp_sum):
    TM_POS = calc_poss(team_sum, opp_sum)
    OP_POS = calc_poss(opp_sum, team_sum)
    TM_MP = minutes(team_sum['MIN'])
    pace = 48 * ((TM_POS + OP_POS) / (2 * (TM_MP / 5)))

    return pace


def minutes(td):
    return td / np.timedelta64(1, 'm')


def history_met(game_id, team_games):
    if (team_games < history_steps):  # check that team has played enough games
        return False

    return True


def iterate_player_list(player_list, team_id, game_id, df_bs, df_teams, league_stats):

    roster_output = []
    error_output = {}
    for player_id in player_list:
        player_game_list, player_teams, df_players = player_prev_games(history_steps, game_id, player_id, df_bs)
        if not player_game_list.size:
            return error_output
        plyr_sum, plyr_avg = calc_basic_stats(player_id, df_players)
        team_sum = calc_team_stats(player_game_list, player_teams, df_teams)
        opp_sum = calc_opp_stats(player_game_list, player_teams, df_teams)
        plyr_advanced = calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats)
        roster_output.append(plyr_avg.values())
        roster_output.append(plyr_advanced.values())

    return roster_output

if __name__ == "__main__":
    start_time = time.time()
    for year in range(min_year, max_year + 1):
        df_lines, df_bs, df_teams = load_dataframes(year)

        for index, row in df_lines.iterrows():
            game_id = index
            if np.isnan(index):
                continue
            home_team_id, away_team_id = get_team_ids(row)
            team_games = min(len(team_prev_games(history_steps, game_id, home_team_id, df_teams)), len(team_prev_games(history_steps, game_id, away_team_id, df_teams)))

            home_player_list, away_player_list = populate_rosters(game_id, home_team_id, away_team_id, df_teams, df_bs)

            if (team_games < history_steps):
                continue
            y, line = get_result(row)
            home_rest, away_rest = get_rest(row)
            league_stats = calc_league_stats(game_id, df_teams)

            home_output = iterate_player_list(home_player_list, home_team_id, game_id, df_bs, df_teams, league_stats)

            if not home_output:
                continue
            away_output = iterate_player_list(away_player_list, away_team_id, game_id, df_bs, df_teams, league_stats)

            if not away_output:
                continue

            print str(int(game_id))

    print "FINISHED"
    print("--- %s seconds ---" % (time.time() - start_time))
