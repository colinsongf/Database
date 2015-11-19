
'''
write_roster.py
function to generate game roster with gameid input

'''
import csv

#function which takes the gameid as a string
def generate_roster(gameid):
	#open the rosterlist by game file
	roster_file = open('rosterlist_all.csv','r') 
	roster_reader = csv.DictReader(roster_file)
	#search through the first column of each row to match the gameid
	rosterlist = []
	for row in roster_reader:
		#if matched, return that row to the function
		if row["gameidnum"] == gameid:
			return row
			
	

#testing the fucntion, should print the right game dict
print(generate_roster('0021501225'))


	


