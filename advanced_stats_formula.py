"""pseudo-code for advanced stats formulas
"""

import sys


def main():
 
 #varianble for how many games you want to calculate the advanced stats by
trailing_games = 5
#set a variable that totals the number of overtime the team played in those trailing games. useful for calculating total minutes
OT_played = #insert something here

#defining regular stats, which will be what advanced stats are based on. Note, these are all TOTAL player specific stats fro the last x number of trailing games
regular_stats = {}
regular_stats[MP] = ["minutes played"]
regular_stats[FGM] = ["field goals made"]
regular_stats[FGA] = ["field goals attempted"]
regular_stats[FGP] = ["field goal percentage"]
regular_stats[3PM] = ["3 pointers made"]
regular_stats[3PA] = ["3 pointers attempted"]
regular_stats[3PP] = ["3 pointers percentage"]
regular_stats[2PM] = ["2 pointers made"]
regular_stats[2PA] = ["2 pointers attempted"]
regular_stats[2PP] = ["2 pointers percentage"]
regular_stats[FTM] = ["free throws made"]
regular_stats[FTA] = ["field goals attempted"]
regular_stats[FTP] = ["field goal percentage"]
regular_stats[ORB] = ["offensive rebound"]
regular_stats[DRB] = ["defensive rebound"]
regular_stats[TRB] = ["total rebound"]
regular_stats[AST] = ["assissts"]
regular_stats[STL] = ["steals"]
regular_stats[BLK] = ["blocks"]
regular_stats[PF] = ["personal fouls"]
regular_stats[PTS] = ["points"]
regular_stats[TOV] = ["turnovers"]

#defining team stats, which are caculated from aggregated player stats, same as above, these are TOTALS from last x number of games. You may need additional formulas here to calculate the totals from your data structure
team_stats={}
team_stats[TM_MP] = ["minutes played"]
team_stats[TM_FGM] = ["field goals made"]
team_stats[TM_FGA] = ["field goals attempted"]
team_stats[TM_FGP] = ["field goal percentage"]
team_stats[TM_3PM] = ["3 pointers made"]
team_stats[TM_3PA] = ["3 pointers attempted"]
team_stats[TM_3PP] = ["3 pointers percentage"]
team_stats[TM_2PM] = ["2 pointers made"]
team_stats[TM_2PA] = ["2 pointers attempted"]
team_stats[TM_2PP] = ["2 pointers percentage"]
team_stats[TM_FTM] = ["free throws made"]
team_stats[TM_FTA] = ["field goals attempted"]
team_stats[TM_FTP] = ["field goal percentage"]
team_stats[TM_ORB] = ["offensive rebound"]
team_stats[TM_DRB] = ["defensive rebound"]
team_stats[TM_TRB] = ["total rebound"]
team_stats[TM_AST] = ["assissts"]
team_stats[TM_STL] = ["steals"]
team_stats[TM_BLK] = ["blocks"]
team_stats[TM_PF] = ["personal fouls"]
team_stats[TM_PTS] = ["total points scored"]
team_stats[TM_TOV] = ["total turnovers"]
team_stats[TM_Pace] = ["team pace"]
TM_Pace = 48*((TM_POS+OP_POS)/(2*(TM_MP/5))
team_stats[TM_ORTG] = ["team offensive rating"]
team_stats[TM_DRTG] = ["team defensive rating"]
team_stats[TM_POS] = ["team total posessions"]
TM_POS = 0.5*((TM_FGA+0.4*TM_FTA-1.07*(TM_ORB/(TM_ORB+OP_DRB))*(TM_FGA-TM_FGM)+TM_TOV)+(OP_FGA+0.4*OP_FTA-1.07*(OP_ORB)/(OP_ORB+TM_DRB))*(OP_FGA-OP_FGM)+OP_TOV)) 

#defining opponent total stats, same as above, totals from last x number of opponents, requires your own formula. 
#note that it'll be sum of all the DIFFERENT opponents that the player's team has played against for the past X games
opponent_team_stats={}
opponent_team_stats[OP_MP] = ["minutes played"]
opponent_team_stats[OP_FGM] = ["field goals made"]
opponent_team_stats[OP_FGA] = ["field goals attempted"]
opponent_team_stats[OP_FGP] = ["field goal percentage"]
opponent_team_stats[OP_3PM] = ["3 pointers made"]
opponent_team_stats[OP_3PA] = ["3 pointers attempted"]
opponent_team_stats[OP_3PP] = ["3 pointers percentage"]
opponent_team_stats[OP_2PM] = ["2 pointers made"]
opponent_team_stats[OP_2PA] = ["2 pointers attempted"]
opponent_team_stats[OP_2PP] = ["2 pointers percentage"]
opponent_team_stats[OP_FTM] = ["free throws made"]
opponent_team_stats[OP_FTA] = ["field goals attempted"]
opponent_team_stats[OP_FTP] = ["field goal percentage"]
opponent_team_stats[OP_ORB] = ["offensive rebound"]
opponent_team_stats[OP_DRB] = ["defensive rebound"]
opponent_team_stats[OP_TRB] = ["total rebound"]
opponent_team_stats[OP_AST} = ["assissts"]
opponent_team_stats[OP_STL] = ["steals"]
opponent_team_stats[OP_BLK] = ["blocks"]
opponent_team_stats[OP_PF] = ["personal fouls"]
opponent_team_stats[OP_PTS] = ["total points opponent scored"]
opponent_team_stats[OP_TOV] = ["total turnovers"]
opponent_team_stats[OP_Pace] = ["team pace"]
TM_Pace = 48*((TM_POS+OP_POS)/(2*(OP_MP/5))
opponent_team_stats[OP_ORTG] = ["team offensive rating"]
opponent_team_stats[OP_DRTG] = ["team defensive rating"]
opponent_team_stats[OP_POS] = ["team total posessions"]
OP_POS = TM_POS

#you're gonna hate me, but we also need to calculate some league wide stats for our time frame
lg_stats={}
lg_stats[LG_FGM] = ["total field goals made"]
lg_stats[LG_FGA] = ["total field goals attempted"]
lg_stats[LG_3PM] = ["total 3 pointers made"]
lg_stats[LG_3PA] = ["total 3 pointers attempted"]
lg_stats[LG_FTM] = ["total free throws made"]
lg_stats[LG_FTA] = ["total free throws attempted"]
lg_stats[LG_ORB] = ["total offensive rebounds"]
lg_stats[LG_DRB] = ["total defensive rebounds"]
lg_stats[LG_TRB] = ["total rebounds"]
lg_stats[LG_AST] = ["total assists"]
lg_stats[LG_PTS] = ["total points"]
lg_stats[LG_TOV] = ["total turnovers"]
lg_stats[LG_PF] = ["total fouls"]
lg_stats[LG_pace] = ["league average pace"]
LG_pace = #sum of pace for all teams divided by 30
lg_stats[LG_possessions] = ["total possessions for all the teams"]
LG_possessions = #sum of all possessions of all the teams
LG_points_per_game = #LG_points dvided by total number of games the league has played



#defining advanced stats
advanced_stats = {}
advanced_stats[PER] = ["player efficiency rating"]
advanced_stats[TS] = ["true shot percentage"]
advanced_stats[3PAr] = ["3 pointers attempt rate"]
advanced_stats[FTr] = ["free throw attempt rate"]
advanced_stats[ORBr] = ["offensive rebounding rate"]
advanced_stats[DRBr] = ["defensive rebounding rate"]
advanced_stats[TRBr] = ["total rebounding rate"]
advanced_stats[ASTr] = ["assisst rate"]
advanced_stats[STLr] = ["steal rate"]
advanced_stats[BLKr] = ["blocking rate"]
advanced_stats[TOVr] = ["turnover rate"]
advanced_stats[USGr] = ["usage rate"]
advanced_stats[OWS] = ["offensive win share"]
advanced_stats[DWS] = ["defensive win share"]
advanced_stats[WS] = ["total win share"]
advanced_stats[WS48] = ["win share per 48 minutes"]
advanced_stats[OBPM] = ["offensive box plus minus"]
advanced_stats[DBPM] = ["defensive box plus minus"]
advanced_stats[BPM] = ["box plus minus"]
advanced_stats[VORP] = ["value over replacement player"]

#formulas for each advanced stats

#PER evalutes a player's overall efficiency on both offense and defense, though it tends to skew towards offensive powerhouses. It's a bitch to calculate
  #first have to define some secondary variables
factor = (2/3)-(0.5*(LG_AST/LG_FGM))/(2*(LG_FGM/LG_FTM))
VOP = LG_PTS/(LG_FGA - LG_ORB+LG_TOV+0.44*LG_FTA) 
DRBp = (LG_TRB - LG_ORB)/LG_TRB
pace_adjust = LG_pace/TM_Pace
unadjusted_PER = (1/MP)*(3PM + (2/3)*AST + (2-factor*(TM_AST/TM_FGM))*FGM + (FTM*0.5*(1+(1-(TM_AST/TM_FGM))+(2/3)*(TM_AST/TM_FGM))) - VOP*TOV - VOP*DRBp*(FGA-FGM) - VOP*0.44*(0.44+(0.56*DRBp))*(FTA-FTM) + VOP*(1-DRBp)*(TRN-ORB) + VOP*DRBp*ORB + VOP*STL + VOP*DRBp*BLK - PF*((LG_FTM/LG_PF) - 0.44*(LG_FTA/LG_PF)*VOP))
  #here's the extra fun part, we need to calculate adjusted PER for every single player, then normalize it so that league average is 15
adjusted_PER = pace_adjust*unadjusted_PER
league_PER = #average of adjusted_per for all players
PER = adjusted_PER*(15/league_PER)

#True shooting percentage is very simple, it's a modified version of field goal percentage that takes into account that 3 points worth 1.5x of 2 pointers as well as free throws. 
#Note that it can exceed 100% if you make a shit ton of 3s

TS = PTS/(2*(FGA + 0.44*FTA))

#3 point attempt rate is also pretty straight forward
3PAr = 3PA/FGA

#free throw attempt rate is the same
FTAr =  FTA/FGA

#offensive rebounding rate is also simple. It's opportunities for all avaible offensive rebounds over offensive rebounds grabbed
ORBr = (ORB*(TM_MP/5))/(MP*(TM_ORB+OP_ORB))

#defensive rebounding rate is similar as above
DRBr = (DRB*(TM_MP/5))/(MP*(TM_DRB+OP_ORB))

#total rebounding rate is similar as above
TRBr = (TRB*(TM_MP/5))/(MP*(TM_TRB+OP_ORB))

#assist rate is similar 
ASTr = AST/(((MP/(TM_MP/5))*TM_FGM)-FGM)

#steal rate
STLr = (STL*(TM_MP/5))/(MP*OP_POS)

#block rate
BLKr = (BLK*(TM_MP/5))/(MP*(OP_FGA - OP_3PA))

#turnover rate is a bit simpler
TOVr = TOV/(FGA+0.44*FTA+TOV)

#usage rate is pretty important, it shows how much the player is being used on offense
USGr = ((FGA+0.44*FTA+TOV)*(TM_MP/5))/(MP*(TM_FGA+0.44*TM_FTA+TM_TOV))

#Offensive winshare measures how many wins the player contributed on offense. It's also complicated and requires a bunch of different parts. 
#note that the value is highly dependent on number of games in scope and may be negative if the team sucks
  #first we need total offensive possessions, which itself have a billion parts
total_off_possessions = scoring_posessions+missed_FG_pos+missed_FT_pos+TOV
scoring posessions = (FG_part+AST_part+FT_part)*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+ORB_part
  #lol, there's more
FG_part = FGM*(1-0.5*((PTS-FTM)/(2*FGA))*qAST)
qAST = ((MP/(TM_MP/5))*(1.14*((TM_AST-AST)/TM_FGM)))+((((TM_AST/TM_MP)*MP*5-AST)/((TM_FGM/TM_MP)*MP*5-FGM))*(1-(MP/(TM_MP/5))))
AST_part = 0.5*(((TM_PTS-TM_FTM)-(PTS-FTM))/(2*(TM_FGA-FGA)))*AST
FT_part = (1-(1-(FTM/FTA))**2)*0.4*FTA
TM_scoring_poss = TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*TM_FTA*0.4
TM_ORB_weight = ((1-TM_ORBr)*TM_plays)/((1-TM_ORBr)*TM_plays+TM_ORBr*(1-TM_plays))
TM_ORBr = TM_ORB/(TM_ORB+(OP_TRB-OP_ORB))
TM_plays = TM_scoring_poss/(TM_FGA+TM_FTA*0.4+TM_TOV)
ORB_part = ORB*TM_ORB_weight*TM_plays

missed_FG_pos = (FGA-FGM)*(1-1.07*TM_ORBr)
missed_FT_pos = ((1-(FTM/FTA))**2)*0.4*FTA
  #still not done yet, now need to calculate points "produced", which is not the same as points scored
points_produced = (PProd_FG_part+PProd_AST_part+FTM)*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+PProd_ORB_part
PProd_FG_part = 2*(FGM+0.5*3PM)*(1-0.5*((PTS-FTM)/(2*FGA))*qAST)
PProd_AST_part = 2*((TM_FGM - FGM +0.5*(TM_3PM-3PM))/(TM_FGM-FGM))*0.5*(((TM_PTS-TM_FTM)-(PTS-FTM))/(2*(TM_FGA-FGA)))*AST
PProd_ORB_part = ORB*TM_ORB_weight*TM_plays*(TM_PTS/(TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*0.4*TM_FTA))
  #almost there
LG_points_per_poss = LG_PTS/LG_possessions
marginal_offense = points_produced-0.92*(LG_points_per_poss)*total_off_possessions 
marginal_pts_per_win = 0.32*(LG_points_per_game)*((TM_Pace)/(LG_pace))
  #finally
OWS = marginal_offense/marginal_pts_per_win

#yay, are you ready now for defensive winshare? which unfortunately very similar to above, just on offense
  #first, need to calculate a player's defensive rating, DRtg
stops = stops1+stops2
stops1 = STL+BLK*FMwt*(1-1.07*DORr)+DRB*(1-FMwt)
FMwt = (DFGr*(1-DORr))/(DFGr*(1-DORr)+(1-DFGr)*DORr)
DORr = OP_ORB/(OP_ORB+TM_DRB)
DFGr = OP_FGM/OP_FGA
stops2 = (((OP_FGA-OP_FGM-TM_BLK)/TM_MP)*FMwt*(1-1.07*DORr)+((OP_TOV-TM_STL)/TM_MP))*MP+(PF/TM_PF)*0.4*OP_FTA*(1-(OP_FTM/OP_FTA))**2
stopr = (stops*OP_MP)/(TM_POS*MP)
DRtg = TM_DRTG+0.2*(100*D_pts_per_ScPoss*(1-stopr)-TM_DRTG)
TM_DRTG = 100*(OP_PTS/TM_POS)
D_pts_per_ScPoss = OP_PTS/(OP_FGM+(1-(1-(OP_FTM/OP_FTA))**2)*OP_FTA*0.4)
  # then similar as above, marginal defense and marginal pts per win
marginal_defense = (MP/TM_MP)*(TM_POS)*(1.08*(LG_points_per_poss)-((DRtg)/100))
DWS = marginal_defense/marginal_pts_per_win

#now, just total winshare, which is thankfully very simple
WS = DWS+OWS

#and WS per 48, also simple
WS48 = WS/((TM_MP/5)/MP)

#now box plus minus, not so simple, but it's basically a regression of other advanced stats
Raw_BPM = co_a*(MP/trailing_games)+co_b*ORBr+co_c*DRBr+co_d*STLr+co_e*BLKr+co_f*ASTr-co_g*USGr*TOVr+co_h*USGr*(1-TOVr)*(2*(TS-TM_TS)+co_i*ASTr+co_j*(3PAr-LG_3PAr)-co_k)+co_l*((ASTr*TRBr)**0.5)
LG_3PAr = LG_3PA/LG_FGA
TM_TS = (TM_PTS - PTS)/(2*((TM_FGA - FGA) + 0.44*(TM_FTA - FTA)))
  #and all the fucking coefficients are
co_a = 0.123391
co_b = 0.119597
co_c = -0.151287
co_d = 1.255644
co_e = 0.531838
co_f = -0.305868
co_g = 0.921292
co_h = 0.711217
co_i = 0.017022
co_j = 0.297639
co_k = 0.213485
co_l = 0.725930
 #but wait, there's more! we need to calulate the team BPM adjustment
 BPM_team_adjust = (team_net_rating*1.2 - raw_BPM_sum)/5
 team_net_rating = TM_ORTG - TM_DRTG
 TM_ORTG = 100*(TM_PTS/TM_POS)
    #calulating raw BPM sum for the team requires you to sum up raw BPM for all the players on the team weighted by their minutes played
    for #each player_id
    raw_BPM_sum = raw_BPM_sum + (MP/(TM_MP/5))*Raw_BPM
#then to adjust BPM 
BPM = Raw_BPM+BPM_team_adjust

#now onto offesnive BPM, which is another regression but with different coefficients
Raw_OBPM = co_oa*(MP/trailing_games)+co_ob*ORBr+co_oc*DRBr+co_od*STLr+co_oe*BLKr+co_of*ASTr-co_og*USGr*TOVr+co_oh*USGr*(1-TOVr)*(2*(TS-TM_TS)+co_oi*ASTr+co_oj*(3PAr-LG_3PAr)-co_ok)+co_ol*((ASTr*TRBr)**0.5)
co_oa = 0.064448
co_ob = 0.211125
co_oc = -0.107545
co_od = 0.346513
co_oe = -0.052476
co_of = -0.041787
co_og = 0.932965
co_oh = 0.687359
co_oi = 0.007952
co_oj = 0.374706
co_ok = -0.181891
co_ol = 0.239862

OBPM = Raw_OBPM + BPM_team_adjust

#DBPM is thankfully easy
DBPM = BPM - OBPM

#finally finally, the last advanced stat, value over replacement player (VORP) calculates the relative value of the player over a league bottom-feeder, which is set to have a BPM of -2.0

VORP = (BPM-(-2.0))*(MP/(TM_MP/5))



