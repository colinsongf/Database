import pandas as pd


if __name__ == "__main__":
    df = pd.read_csv('output.csv')
    labels = df.columns.values.tolist()
    output = ''
    for label in labels:
        output = output + ' + ' + str(label)
    print output
    f = open('header.txt', 'w')
    f.write(output)
    f.close()
