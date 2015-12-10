import pandas as pd
from sklearn import preprocessing
# import csv
# import json


def import_data():
    df1 = pd.read_csv('./processedoutput.csv')
    # df2 = pd.read_csv('./teamlist.csv', index_col=['Team'])
    return df1


if __name__ == "__main__":
    df = import_data()

    output = df.drop_duplicates(['Team'])
    print output.head()
    # output = df.iloc[:, 8].drop_duplicates()
    # print df.iloc[:, 9].drop_duplicates()
    # for x in range(8, 227):
    # output = pd.concat([output, df.iloc[:, 9].drop_duplicates()], axis=1)
    output.to_csv('processedoutput.csv')
    print "finished"
