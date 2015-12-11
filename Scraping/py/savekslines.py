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
teamNames = {'1610612737': 'Hawks', '1610612738':'Celtics', '1610612751':'Nets', '1610612766':'Hornets','1610612741':'Bulls','1610612739':'Cavaliers','1610612742':'Mavericks','1610612743':'Nuggets','1610612765':'Pistons','1610612744':'Warriors','1610612745':'Rockets','1610612754':'Pacers','1610612746':'Clippers','1610612747':'Lakers','1610612763':'Grizzlies','1610612748':'Heat','1610612749':'Bucks','1610612750':'Timberwolves','1610612740':'Pelicans','1610612752':'Knicks','1610612760':'Thunder','1610612753':'Magic','1610612755':'Seventysixers','1610612756':'Suns','1610612757':'Trailblazers','1610612758':'Kings','1610612759':'Spurs','1610612761':'Raptors','1610612762':'Jazz','1610612764':'Wizards'}
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
	nullcounter = 5
	for lines in stringtable:
		spacecounter = spacecounter +1
		if spacecounter >2:
			
			f.write(lines.strip().replace(',','')+',')
			linecounter = linecounter + 1
			nullcounter = nullcounter + 1
			if linecounter ==tablewidth:
				f.write(gameidnum+'\n')
				linecounter = 0
			
			if lines =='\n':
				nullcounter = 0
			if nullcounter == 3:
				f.write(',,,,,,,,,'+gameidnum+'\n')		
				linecounter = 0
				nullcounter = 5
			
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