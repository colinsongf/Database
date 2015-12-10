import pandas as pd
# import data as data


def atsr_test(game_data):
    filepath = ('./data/linetestdata.csv')
    df = pd.read_csv(filepath, index_col='game_id')
    success = True
    failures = []
    for game_id, row in df.itterrows():
        if (row['ATSr'] == 'W') and (game_data[game_id]['y'] == 0):
            success = False
            failures.append(game_id)
        if (row['ATSr'] != 'W') and (game_data[game_id]['y'] == 1):
            success = False
            failures.append(game_id)
    if (success):
        print "PASS"
    else:
        print "FAILED: " + failures

    return success

def boxscore_test(game_data):
    filepath = ('./')
