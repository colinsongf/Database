'''
scrapes latest play type by team from stats.nba.com
'''

import re
import dryscrape
import urllib2
from bs4 import BeautifulSoup

#file save path
SavePath = '/media/sf_GitHub/Database/Scraping/results'

#defining team name to abbrev for later
teamNames = {'ATL': 'Atlanta Hawks', 'BOS':'Boston Celtics', 'BKN':'Brooklyn Nets', 'CHA':'Charlotte Hornets','CHI':'Chicago Bulls','CLE':'Cleveland Cavaliers','DAL':'Dallas Mavericks','DEN':'Denver Nuggets','DET':'Detroit Pistons','GSW':'Golden State Warriors','HOU':'Houston Rockets','IND':'Indiana Pacers','LAC':'Los Angeles Clippers','LAL':'Los Angeles Lakers','MEM':'Memphis Grizzlies','MIA':'Miami Heat','MIL':'Milwaukee Bucks','MIN':'Minnesota Timberwolves','NOP':'New Orlenas Pelicans','NYK':'New York Knicks','OKC':'Oklahoma City Thunder','ORL':'Orlando Magic','PHI':'Philadelphia 76ers','PHX':'Phoenix Suns','POR':'Portland Trail Blazers','SAC':'Sacramento Kings','SAS':'San Antonio Spurs','TOR':'Toronto Raptors','UTA':'Utah Jazz','WAS':'Washington Wizards'}  

# play types to cycle through
playtypes = ['transition', 'isolation', 'ball-handler', 'roll-man', 'post-up', 'spot-up', 'hand-off', 'cut', 'off-screen', 'put-backs', 'misc']
playside = ['offensive', 'defensive']

#stats url

nbaurl = u'http://stats.nba.com'

# make sure you have xvfb installed
dryscrape.start_xvfb()

# set up a web scraping session
sess = dryscrape.Session(base_url = nbaurl)
# we don't need images
sess.set_attribute('auto_load_images', False)

#opens the page and loads js, then pass all text to beautifulsoup
sess.visit('/playtype/#!/transition/?PT=team&OD=defensive')

response = sess.body()

soup = BeautifulSoup(response)

rawvalues = soup.get_text()

#find where the table starts and ends
start_match = re.search("Percentile", rawvalues)
end_match = re.search("AboutAbout", rawvalues)

trunctedvalue = rawvalues[start_match.end():end_match.start()]
nospacevalue = "\n".join([ll.rstrip() for ll in trunctedvalue.splitlines() if ll.strip()])

print (nospacevalue)


