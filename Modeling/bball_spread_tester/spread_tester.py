# import csv
# import json
# import numpy
import pandas as pd


''' Takes a CSV input from wagers.csv and calculates the results and ROI
of a given set of bets, checked against vegas line data for a chosen season.

For wagers.csv, legend as follows
    game_id: game id MUST correspond with id in line data. If there are any
duplicate game ids, the calculation will not work.
    home_wins: 'W' if you bet on the home team to cover, 'L' if you bet on the
away team to cover
    odds: Odds in standard US format, -110 as default
    amount: amount bet in UNITS. Standard is 1 unit. Just a note to avoid
any confusion, if your amount is '100' that means you are betting 100 times
your normal betting size!
'''


def import_lines(year_indicator):  # imports csv file with line data
    linedata_filepath = ('./linedata/NBAlines' + year_indicator +
                         '.csv')
    df1 = pd.read_csv(linedata_filepath, index_col='game_id')
    return df1


def import_wagers():  # imports csv file with games to be bet on
    wagers_filepath = './linedata/wagers.csv'
    df2 = pd.read_csv(wagers_filepath, index_col='game_id')
    return df2


def calculate_roi(df_roi):  # calculates roi based on winning/losing bets
    profit, buyins = 0, 0
    for game_id, row in df_roi.iterrows():
        buyins += (float(row.loc['amount']) *
                   float(row.loc['odds']) * -1)
        if row.loc['result'] == 1:  # adds winnings if bet won
            profit += float(row.loc['amount'])
        else:  # subtracts buyin if bet lost
            profit += (float(row.loc['amount']) *
                       float(row.loc['odds']) / 100)
    buyins = buyins / 100  # adjusting the magnitude
    return profit / buyins


if __name__ == "__main__":
    year_indicator = '2014'  # replace with season year or 'all'
    df1 = import_lines(year_indicator)
    df2 = import_wagers()  # imports the csv files into pandas dataframes
    num_wagers, num_wins = 0, 0
    num_games = len(df1.index)

    for game_id, row in df2.iterrows():  # iterates through the bets made
        num_wagers += 1
        if (df1.loc[game_id, 'ATSr'] == row.loc['home_wins']):
            df2.loc[game_id, 'result'] = 1  # records if the bet won
            num_wins += 1
        elif df1.loc[game_id, 'ATSr'] == 'P':
            df2.loc[game_id, 'result'] = 0
        else:
            df2.loc[game_id, 'result'] = 0
    roi = calculate_roi(df2)*100
    output = ("Games in season: %d\nBets: %d\nWins: %d\nROI: %.2f%%"
              % (num_games, num_wagers, num_wins, roi))
    print output
