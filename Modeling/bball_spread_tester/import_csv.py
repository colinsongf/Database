# import csv
# import json
# import numpy
import pandas as pd


def import_csv(year_indicator):
    df1 = pd.read_csv('~/Documents/linedata/NBAlines' + year_indicator +
                      '.csv', index_col='game_id')
    return df1


def import_wagers():
    df2 = pd.read_csv('~/Documents/linedata/wagers.csv', index_col='game_id')
    return df2


def calculate_roi(df_roi):
    winnings, buyins = 0
    for game_id in df_roi:
        buyins += (float(df_roi.loc[game_id, 'amount']) *
                   float(df_roi.loc[game_id, 'odds']) * -1)
        if df_roi.loc[game_id, 'result']:
            winnings += float(df_roi.loc[game_id, 'amount'])
    return winnings / buyins


if __name__ == "__main__":
    df1 = import_csv('2014')
    df2 = import_wagers()
    aggregate = pd.concat(df1, df2)

    num_wagers, num_wins = 0
    num_games = len(df1.index)
    for game_id in df2:
        num_wagers += 1
        if (df1.loc[game_id, 'ATSr'] == df2.loc[game_id, 'home_wins']):
            df2.loc[game_id, 'result'] = 1
            num_wins += 1
        else:
            df2.loc[game_id, 'result'] = 0
    roi = calculate_roi(df2)
    output = ("Num. of games: %d\nNum. of bets: %d\nNum. of wins: %d\nROI: %f"
              % (num_games, num_wagers, num_wins, roi))
    print output
