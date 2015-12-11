
'''
rosterlist.py
extract game info from BS JSON files

'''


import simplejson as json
import sys
import csv

#path of json files
DATAPATH = '/media/sf_Public/NBA_data/json'
#change here to output either player name or player id, 5 is name, 4 is id
id_name = 4

f = open('roster_by_game.csv', 'w')
f.write('gameid,home_team_id,away_team_id,home_player1,home_player2,home_player3,home_player4,home_player5,home_player6,home_player7,home_player8,home_player9,home_player10,home_player11,home_player12,home_player13,home_player14,home_player15,away_player1,away_player2,away_player3,away_player4,away_player5,away_player6,away_player7,away_player8,away_player9,away_player10,away_player11,away_player12,away_player13,away_player14,away_player15'+'\n')

def write_roster_to_row(gameid):
	#open bs josn file from list of games
	print 'opening bs_%s.json' % (gameid)

	json_data = open('%s/bs_%s.json' % (DATAPATH,gameid)).read()

	bs_data = json.loads(json_data)
	#parsing headers, borrowed from terry
	game_summary_header = bs_data[0]["headers"]
	game_summary = bs_data[0]["rowSet"][0]
	line_score_header = bs_data[1]["headers"]
	line_score = bs_data[1]["rowSet"]
	player_stats_header = bs_data[4]['headers']
	player_stats = bs_data[4]["rowSet"]

	# determining teams and players
	home_team_index = game_summary_header.index('HOME_TEAM_ID')
	away_team_index = game_summary_header.index('VISITOR_TEAM_ID')
	gameid_index = game_summary_header.index('GAME_ID')


	home_team = game_summary[home_team_index]
	away_team = game_summary[away_team_index]
	game_id = game_summary[gameid_index]

	index_x = 0
	index_y = 0
	home_player_count = 0
	
	f.write(str(game_id)+','+str(home_team)+','+str(away_team)+',')
	#write players in a row
	while index_x < len(player_stats):

		current_player=player_stats[index_x]
		if current_player[1] == home_team:
			print current_player[id_name]
			f.write(str(current_player[id_name])+',')
			home_player_count = home_player_count + 1

		index_x = index_x + 1  
	while home_player_count < 15:
		f.write(',')
		home_player_count = home_player_count + 1

	while index_y < len(player_stats):

		current_player=player_stats[index_y]
		if current_player[1] == away_team:
			print current_player[id_name]
			f.write(str(current_player[id_name])+',')
		index_y	= index_y + 1
	f.write('\n')		
	

def write_gamelist_json(gamelist):
  f = open(gamelist, 'r')
  f.readline() # drop headers    
  for r in f.readlines():
    gameid = r.split(',')[0]
    write_roster_to_row(gameid)

def main():
  #write_game_json(sys.argv[1]) 
  write_gamelist_json(sys.argv[1]) 

f.close
if __name__ == '__main__': 
 main()
