'''
scrapes latest play type by team from stats.nba.com
'''

import re
import dryscrape
import urllib2
from bs4 import BeautifulSoup
import StringIO
import time

#file save path
SavePath = '/media/sf_GitHub/Database/Scraping/results'

#defining team name to abbrev for later
teamNames = {'ATL': 'Atlanta Hawks', 'BOS':'Boston Celtics', 'BKN':'Brooklyn Nets', 'CHA':'Charlotte Hornets','CHI':'Chicago Bulls','CLE':'Cleveland Cavaliers','DAL':'Dallas Mavericks','DEN':'Denver Nuggets','DET':'Detroit Pistons','GSW':'Golden State Warriors','HOU':'Houston Rockets','IND':'Indiana Pacers','LAC':'Los Angeles Clippers','LAL':'Los Angeles Lakers','MEM':'Memphis Grizzlies','MIA':'Miami Heat','MIL':'Milwaukee Bucks','MIN':'Minnesota Timberwolves','NOP':'New Orlenas Pelicans','NYK':'New York Knicks','OKC':'Oklahoma City Thunder','ORL':'Orlando Magic','PHI':'Philadelphia 76ers','PHX':'Phoenix Suns','POR':'Portland Trail Blazers','SAC':'Sacramento Kings','SAS':'San Antonio Spurs','TOR':'Toronto Raptors','UTA':'Utah Jazz','WAS':'Washington Wizards'}  

# play types to cycle through
playtypes = ['isolation', 'transition', 'ball-handler', 'roll-man', 'post-up', 'spot-up', 'hand-off', 'cut', 'off-screen', 'putbacks', 'misc']
playside = ['offensive', 'defensive']

#stats url

nbaurl = u'http://stats.nba.com'

#prep the output file
f = open('playtypes.csv','w')
f.write('Team,GP,Poss,Freq,PPP,PTS,FGM,FGA,FG%,eFG%,FT_Freq,TO_Freq,SF_Freq,AndOne_Freq,Score_Freq,Percentile,Playtype,Side'+'\n')

# make sure you have xvfb 
dryscrape.start_xvfb()
# set up a web scraping session
sess = dryscrape.Session(base_url = nbaurl)
# we don't need images
sess.set_attribute('auto_load_images', False)

#iterate through all the playtypes and sides
for d1 in playside:

	for f1 in playtypes:
		print('scraping '+d1+' '+f1)
			#opens the page and loads js, then pass all text to beautifulsoup
		sess.visit('/playtype/#!/'+f1+'/?PT=team&OD='+d1)
		tabletype = f1
		OorD = d1

		#wait a bit cuz javascript is slow
		time.sleep(5)
		response = sess.body()
		#pass the js loaded webpage to beautifulsoup
		soup = BeautifulSoup(response,'lxml')
		rawvalues = soup.get_text()
		
		#find where the data table starts and ends
		start_match = re.search("Percentile", rawvalues)
		end_match = re.search("AboutAbout", rawvalues)

		#extract the table and get rid of spaces
		trunctedvalue = rawvalues[start_match.end():end_match.start()]
		nospacevalue = "\n".join([ll.rstrip() for ll in trunctedvalue.splitlines() if ll.strip()])

		#write the values to file, also defining the table width so it knows when to start a new row
		stringtable = StringIO.StringIO(nospacevalue)
		tablewidth = 16
		linecounter = 0
		for line in stringtable:
			f.write(line.strip()+",")
			linecounter = linecounter + 1
			if linecounter == tablewidth:
				f.write(tabletype+','+OorD+'\n')
				linecounter = 0	


print ('all done')