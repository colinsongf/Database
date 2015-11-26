'''
scrapes game info and lines from killersports

'''
import re
import dryscrape
import urllib2
from bs4 import BeautifulSoup
import StringIO
import time
import sys

#stats url

ksurl = u'http://killersports.com'


#defining team name to abbrev for later
teamNames = {'ATL': 'Hawks', 'BOS':'Celtics', 'BKN':'Nets', 'CHA':'Hornets','CHI':'Bulls','CLE':'Cavaliers','DAL':'Mavericks','DEN':'Nuggets','DET':'Pistons','GSW':'Warriors','HOU':'Rockets','IND':'Pacers','LAC':'Clippers','LAL':'Lakers','MEM':'Grizzlies','MIA':'Heat','MIL':'Bucks','MIN':'Timberwolves','NOP':'Pelicans','NYK':'Knicks','OKC':'Thunder','ORL':'Magic','PHI':'Seventysixers','PHX':'Suns','POR':'Trailblazers','SAC':'Kings','SAS':'Spurs','TOR':'Raptors','UTA':'Jazz','WAS':'Wizards'}
#prep the output file
f = open('NBA OU data (since 2000).csv','a')


# make sure you have xvfb 
dryscrape.start_xvfb()
# set up a web scraping session
sess = dryscrape.Session(base_url = ksurl)
# we don't need images
sess.set_attribute('auto_load_images', False)

#define date range

def write_lines_by_date(date,hometeam,gameidnum):

	sess.visit('http://killersports.com/nba/query?sdql=season%3D2015+and+site%3Dhome+and+date%3D'+date+'+and+team%3D'+hometeam+'&submit=++S+D+Q+L+%21++')
	time.sleep(2)
	response = sess.body()
	#pass the js loaded webpage to beautifulsoup
	soup = BeautifulSoup(response,'lxml')
	rawvalues = soup.get_text()

	#find where the data table starts and ends
	start_match = re.search("ATSrOUrot", rawvalues)
	end_match = re.search("Showing 1", rawvalues)

	#extract the table and get rid of spaces
	trunctedvalue = rawvalues[start_match.end():end_match.start()]


	nospacevalue = "\n".join([ll.rstrip() for ll in trunctedvalue.splitlines() if ll.strip()])
	stringtable = StringIO.StringIO(nospacevalue.replace('  ','\n'))

	linecounter = 0
	tablewidth = 20
	spacecounter = 0
	for lines in stringtable:
		spacecounter = spacecounter +1
		if spacecounter >2:
			
			f.write(lines.strip().replace(',','')+',')
			linecounter = linecounter + 1
			if linecounter ==tablewidth:
				f.write(gameidnum+'\n')
				linecounter = 0
	print ('wrote '+gameidnum+', '+hometeam)

#write_lines_by_date('20151124','Warriors',1991919)
	
def read_gamelist(gamelist):
  f = open(gamelist, 'r')
  print('opening')
  f.readline() # drop headers    
  for r in f.readlines():
    gameid = r.split(',')[0]
    hteamabvr = r.split(',')[3]
    hteam = teamNames[hteamabvr]
    dateid = r.split(',')[2]
    dates = dateid[0:8]
    write_lines_by_date(dates,hteam,gameid)

def main():
   read_gamelist(sys.argv[1]) 

if __name__ == '__main__': 
  main()