'''
scrapes latest play type by team from stats.nba.com
'''

import re
from bs4 import BeautifulSoup
import urllib2

#file save path
DATAPATH = 'C:/Users/Ke/Dropbox/Public/NBA_data/playtype'

#defining team name to abbrev for later
teamNames = {'ATL': 'Atlanta Hawks', 'BOS':'Boston Celtics', 'BKN':'Brooklyn Nets', 'CHA':'Charlotte Hornets','CHI':'Chicago Bulls','CLE':'Cleveland Cavaliers','DAL':'Dallas Mavericks','DEN':'Denver Nuggets','DET':'Detroit Pistons','GSW':'Golden State Warriors','HOU':'Houston Rockets','IND':'Indiana Pacers','LAC':'Los Angeles Clippers','LAL':'Los Angeles Lakers','MEM':'Memphis Grizzlies','MIA':'Miami Heat','MIL':'Milwaukee Bucks','MIN':'Minnesota Timberwolves','NOP':'New Orlenas Pelicans','NYK':'New York Knicks','OKC':'Oklahoma City Thunder','ORL':'Orlando Magic','PHI':'Philadelphia 76ers','PHX':'Phoenix Suns','POR':'Portland Trail Blazers','SAC':'Sacramento Kings','SAS':'San Antonio Spurs','TOR':'Toronto Raptors','UTA':'Utah Jazz','WAS':'Washington Wizards'}  

# play types to cycle through
playtypes = ['transition', 'isolation', 'ball-handler', 'roll-man', 'post-up', 'spot-up', 'hand-off', 'cut', 'off-screen', 'put-backs', 'misc']
playside = ['offensive', 'defensive']

#stats url

url = u'http://killersports.com/nba/query?sdql=season%3D2015+and+team%3DKnicks&submit=++S+D+Q+L+%21++'

content = urllib2.urlopen(url).read()

pagetext = BeautifulSoup(content)

print(pagetext.get_text())
