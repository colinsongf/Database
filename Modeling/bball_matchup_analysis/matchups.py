import pandas as pd


def import_data():
    df1 = pd.read_csv('./playtypedata/NBALines2014.csv', index_col=['Team',
                      'Opp', 'game_id'])
    # df2 = pd.read_csv('./teamlist.csv', index_col=['Team'])
    return df1


if __name__ == "__main__":
    weak_against = ['Heat', 'Clippers', 'Bucks']
    strong_against = ['Grizzlies', 'Jazz', 'Nuggets', 'Bulls']
    # strong_against = ['Pelicans', 'Magic', 'Timberwolves', 'Wizards']
    high_freq = ['Magic', 'Hawks', 'Pelicans', 'Celtics']  # and cavs
    df = import_data()
    output = pd.DataFrame()
    outstr = pd.DataFrame()
    df = df.sortlevel(0)
    for team1 in weak_against:
        for team2 in high_freq:
            if team1 != team2:
                df.loc[(team1, team2), 'Result'] = 'L'
                df.loc[(team2, team1), 'Result'] = 'W'
                output = output.append(df.loc[team1, team2])
                output = output.append(df.loc[team2, team1])

    for team1 in strong_against:
        for team2 in high_freq:
            if team1 != team2:
                df.loc[(team1, team2), 'Result'] = 'W'
                df.loc[(team2, team1), 'Result'] = 'L'
                outstr = outstr.append(df.loc[team1, team2])
                outstr = outstr.append(df.loc[team2, team1])
    output.drop(output.columns[2:13], 1, inplace=True)
    outstr.drop(outstr.columns[2:13], 1, inplace=True)
    path1 = 'pnrroll.csv'
    path2 = 'pnrrollstr.csv'
    output.to_csv(path1)
    df2 = pd.read_csv(path1, index_col=['game_id'])
    for id, row in df2.iterrows():
        df2.loc[id, 'Bet'] = 1 if (row['ATSr'] == row['Result']) else 0
    df2.to_csv(path1)

    outstr.to_csv(path2)
    df3 = pd.read_csv(path2, index_col=['game_id'])
    for id, row in df3.iterrows():
        df3.loc[id, 'Bet'] = 1 if (row['ATSr'] == row['Result']) else 0
    df3.to_csv(path2)

    print "finished"
