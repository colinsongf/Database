#!/usr/bin/python
import requests # query web
import json # parse json

if __name__ == '__main__':

  JSONPATH = '/home/gmf/Code/repos/nba/json'
  sv_url = 'http://stats.nba.com/stats/locations_getmoments/'
  sv_params = {'eventid':0, 'gameid':0}

  for gamenum in range(111, 115):
    #gameid = '00214' + str(gamenum).zfill(5)
    gameid = '00414' + str(gamenum).zfill(5)
    pbp_file = open(JSONPATH + '/pbp_' + gameid + '.json','r')
    pbp = json.loads(pbp_file.read())['rowSet']
    sv_params['gameid'] = gameid
    
    eventids = []
    for p in pbp[1:]:
      eventids.append(p[1])
    print "Number of events: " + str(eventids[-1])
    
    # loop over events, get sport vu data for each one, keep track of errors
    errlist = []
    for eventid in eventids:
      print str(eventid)
      try:
        sv_params['eventid'] = eventid
        j = requests.get(sv_url, params=sv_params).json()
        json_file = JSONPATH + '/sv_' + gameid + '_' + str(eventid).zfill(4) + '.json'
        f = open(json_file, 'w')
        json.dump(j, f)
        f.close()
      except IOError:
        errlist.append(str(eventid))
        print "Error on " + str(eventid)
        
    fw = open('errlist.txt','a')
    fw.write('\n'.join(errlist))
    fw.close()
