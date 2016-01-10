import pandas as pd
import numpy as np
import tables
import math
import time
import timeit
import collections
import pickle
import sys
from random import randint
import utilities


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


def calc_league_stats(league_sum, num_games):
    '''
    calc_league_stats: Takes in a dict of summed league stats up through a certain date, outputs league pps, pace, ppg. Used for advanced stats.
    '''
    league_dict = {}

    # initialize team stats for easier formulas
    TM_FGA, TM_FTA = league_sum['FGA'], league_sum['FTA']
    TM_FGM, TM_TOV = league_sum['FGM'], league_sum['TO']
    TM_ORB, TM_DRB = league_sum['OREB'], league_sum['DREB']
    OP_FGA, OP_FTA, OP_FGM, OP_ORB, OP_DRB, OP_TOV = TM_FGA, TM_FTA, TM_FGM, TM_ORB, TM_DRB, TM_TOV

    league_possessions = 0.5 * ((TM_FGA + 0.4 * TM_FTA - 1.07 * (TM_ORB / (TM_ORB + OP_DRB)) * (TM_FGA - TM_FGM) + TM_TOV) + (OP_FGA + 0.4 * OP_FTA - 1.07 * (OP_ORB / (OP_ORB + TM_DRB)) * (OP_FGA - OP_FGM) + OP_TOV))

    league_dict['PPG'] = league_sum['PTS'] / num_games
    league_dict['PACE'] = 48 * ((league_possessions * 2) / (2 * (league_sum['MIN'] / 5)))

    league_dict['PPS'] = league_sum['PTS'] / (league_sum['FGA'] + 0.44 * league_sum['FTA'])

    return league_dict


def calc_advanced_stats(plyr_sum, team_sum, opp_sum, league_stats):
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
    # plyr_advanced['PACE'] = TM_PACE

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
