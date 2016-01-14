'''
generate player pps by shot location by game
each row will be one player
columns will be shot attempt by location and pps by location

'''


import pandas as pd
import tables
import sys
import csv
import numpy as np
import re

# prepare csv file
f = open('xefg_by_player.csv', 'w')
writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
f.write('player_id,player_name,player_size,team_name,team_id,location,game_id,Date,Season,atb3_pts,atb3_attempt,atb3_pps,c3_pts,c3_attempt,c3_pps,mid_pts,mid_attempt,mid_pps,ra_pts,ra_attempt,ra_pps,paint_pts,paint_attempt,paint_pps,total_pps'+'\n')

#need to change carmelo to big for '12 and '13 reason, and beasly to big for game 021201227

#get teamid, location, and player size

psize = pd.read_csv('player_size.csv')
loc = pd.read_csv('game_loc.csv')

# defining data location
DATAPATH = '/media/sf_GitHub/Database/Modeling/processor/data'
for seasons in range(2000,2015):

    df_shots= pd.read_hdf('%s/shots/shots%s.h5' % (DATAPATH,str(seasons)[2:4]))
    df_bs = pd.read_hdf('%s/players/bs%s.h5' % (DATAPATH,str(seasons)[2:4]))


    for gameid in pd.unique(df_shots['GAME_ID'].values):
        #confine both files to specific game
        df_shots_game = df_shots[df_shots['GAME_ID']==gameid]
        date = df_shots_game['Date'].mean()
        df_bs_game = df_bs[df_bs['GAME_ID']==gameid]
        print (gameid)
        loc_game = loc[loc['gameid']==gameid]

        # get playerlist to iterate later
        playerlist = df_shots_game['PLAYER_ID'].unique()

        # get shots by area
        fgm_by_area = df_shots_game.SHOT_MADE_FLAG.groupby([df_shots_game.PLAYER_ID,df_shots_game.SHOT_ZONE_BASIC]).sum()
        fga_by_area = df_shots_game.SHOT_ATTEMPTED_FLAG.groupby([df_shots_game.PLAYER_ID,df_shots_game.SHOT_ZONE_BASIC]).sum()

        # get fta and ftm from bs
        try:
            fta = df_bs_game.FTA.groupby(df_bs_game.PLAYER_ID).sum()
        except KeyError:
            fta = 0
        try:
            ftm = df_bs_game.FTM.groupby(df_bs_game.PLAYER_ID).sum()
        except KeyError:
            ftm = 0

        #for each player, get shots attempted by area so we can peanut butter the fts
        for players in playerlist:

            player_size = psize[psize['ID']==players]['Size'].values
            try:
                playersize = player_size[0]
            except     IndexError:
                playersize = 'nan'
            teamid = df_bs_game[df_bs_game['PLAYER_ID']==players]['TEAM_ID'].mean()
            location1 = loc_game[loc_game['teamid']==teamid]['location'].values
            try:
                location = location1[0]
            except IndexError:
                location = 'nan'

            #get shots attempt and made by area, set to zero if not found
            try:
                player_atb3_at = fga_by_area[(players,'Above the Break 3')]
            except KeyError:
                player_atb3_at = 0
            try:
                player_atb3_m = fgm_by_area[(players,'Above the Break 3')]
            except KeyError:
                player_atb3_m = 0
            try:
                player_c3r_at = fga_by_area[(players,'Right Corner 3')]
            except KeyError:
                player_c3r_at = 0
            try:
                player_c3r_m = fgm_by_area[(players,'Right Corner 3')]
            except KeyError:
                player_c3r_m = 0
            try:
                player_c3l_at = fga_by_area[(players,'Left Corner 3')]
            except KeyError:
                player_c3l_at = 0
            try:
                player_c3l_m = fgm_by_area[(players,'Left Corner 3')]
            except KeyError:
                player_c3l_m = 0
            player_c3_at = player_c3l_at + player_c3r_at
            player_c3_m = player_c3l_m + player_c3r_m
            try:
                player_mid_at = fga_by_area[(players,'Mid-Range')]
            except KeyError:
                player_mid_at = 0
            try:
                player_mid_m = fgm_by_area[(players,'Mid-Range')]
            except KeyError:
                player_mid_m = 0
            try:
                player_ra_at = fga_by_area[(players,'Restricted Area')]
            except KeyError:
                player_ra_at = 0
            try:
                player_ra_m = fgm_by_area[(players,'Restricted Area')]
            except KeyError:
                player_ra_m = 0
            try:
                player_paint_at = fga_by_area[(players,'In The Paint (Non-RA)')]
            except KeyError:
                player_paint_at = 0
            try:
                player_paint_m = fgm_by_area[(players,'In The Paint (Non-RA)')]
            except KeyError:
                player_paint_m = 0

            #calculate theoretical fts based on shot selection
            try:
                player_ftm = ftm[players]
            except KeyError:
                player_ftm = 0
            theoritical_fta = 0.0125*player_atb3_at*3+0.01*player_c3_at*3+0.052*player_mid_at*2+0.22*player_ra_at*2+0.1855*player_paint_at*2
            try:
                extra_ft_ratio = player_ftm/theoritical_fta
            except ZeroDivisionError:
                extra_ft_ratio = 0

            #calculate adjusted pps by area, set to NaN if no shots attempted
            player_atb3_pts = player_atb3_m*3+extra_ft_ratio*0.0125*player_atb3_at*3
            try:
                player_atb3_pps = player_atb3_pts/player_atb3_at
            except ZeroDivisionError:
                player_atb3_pps = np.nan


            player_c3_pts = player_c3_m*3+extra_ft_ratio*0.01*player_c3_at*3
            try:
                player_c3_pps = player_c3_pts/player_c3_at
            except ZeroDivisionError:
                player_c3_pps = np.nan

            player_mid_pts = player_mid_m*2+extra_ft_ratio*0.052*player_mid_at*2
            try:
                player_mid_pps = player_mid_pts/player_mid_at
            except ZeroDivisionError:
                player_mid_pps = np.nan

            player_ra_pts = player_ra_m*2+extra_ft_ratio*0.22*player_ra_at*2
            try:
                player_ra_pps = player_ra_pts/player_ra_at
            except ZeroDivisionError:
                player_ra_pps = np.nan

            player_paint_pts = player_paint_m*2+extra_ft_ratio*0.1855*player_paint_at*2
            try:
                player_paint_pps = player_paint_pts/player_paint_at
            except ZeroDivisionError:
                player_paint_pps = np.nan

            try:
                player_total_pps = df_bs_game[df_bs_game['PLAYER_ID']==players]['PTS'].sum()/df_bs_game[df_bs_game['PLAYER_ID']==players]['FGA'].sum()
            except ZeroDivisionError:
                player_total_pps = 0

            #get player name and team name
            try:
                player_name = df_bs_game[df_bs_game['PLAYER_ID']==players]['PLAYER_NAME'].values[0]
            except KeyError:
                player_name = 'nan'
            except IndexError:
                player_name = 'nan'
            try:
                team_name = df_shots_game[df_shots_game['PLAYER_ID']==players]['TEAM_NAME'].values[0]
            except KeyError:
                team_name = 'nan'
            except IndexError:
                team_name = 'nan'


            #write everything into file
            if player_name == 'nan':
                x=1
            else:
                writer.writerow([players,player_name,playersize,team_name,teamid,location,gameid,date,seasons,player_atb3_pts,player_atb3_at,player_atb3_pps,player_c3_pts,player_c3_at,player_c3_pps,player_mid_pts,player_mid_at,player_mid_pps,player_ra_pts,player_ra_at,player_ra_pps,player_paint_pts,player_paint_at,player_paint_pps,player_total_pps])



        print('done')
