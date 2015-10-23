import pandas as pd
# import csv
# import json


def import_data():
    df1 = pd.read_csv('./playtypedata/playtype_offense.csv',
                      index_col=['Team', 'play_type'])
    df2 = pd.read_csv('./playtypedata/playtype_defense.csv',
                      index_col=['Team', 'play_type'])
    year_indicator = '2014'
    df3 = pd.read_csv('./playtypedata/NBAlines' + year_indicator +
                      '.csv', index_col='game_id')
    return df1, df2, df3


if __name__ == "__main__":
    df_offense, df_defense, df_lines = import_data()
    df_output = df_lines.loc[:, 'Team':'ATSr']
    df_output.drop(['Site', 'Rest', 'Total', 'OUm', 'DPS', 'DPA'],
                   inplace=True, axis=1)  # del unwanted columns
    home_team, away_team, column_header = '', '', ''  # init empty strings
    factors = ['Poss', 'Freq', 'eFG', 'FTfreq', 'TOfreq']  # data per playtype
    for game_id, game_row in df_lines.iterrows():
        home_team = game_row.loc['Team']
        away_team = game_row.loc['Opp']

        # home team offense
        for playtype, play_row in df_offense.loc[home_team].iterrows():
            for factor in factors:
                column_header = 'home_' + 'off_' + playtype + '_' + factor
                df_output.loc[game_id, column_header] = play_row.loc[factor]

        # home team defense
        for playtype, play_row in df_defense.loc[home_team].iterrows():
            for factor in factors:
                column_header = 'home_' + 'def_' + playtype + '_' + factor
                df_output.loc[game_id, column_header] = play_row.loc[factor]

        # away team offense
        for playtype, play_row in df_offense.loc[away_team].iterrows():
            for factor in factors:
                column_header = 'away_' + 'off_' + playtype + '_' + factor
                df_output.loc[game_id, column_header] = play_row.loc[factor]

        # away team defense
        for playtype, play_row in df_defense.loc[away_team].iterrows():
            for factor in factors:
                column_header = 'away_' + 'def_' + playtype + '_' + factor
                df_output.loc[game_id, column_header] = play_row.loc[factor]

    df_output.to_csv('output.csv')
    print "finished"
