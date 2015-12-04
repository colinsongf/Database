# imports
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from psycopg2.extras import DictCursor
from sklearn import metrics
from patsy import dmatrices
from random import random
from pprint import pprint
import pandas as pd
import numpy as np
import psycopg2
import warnings
import cPickle
import math
import copy
import json
import time
import sys



##############################################
###  FUNCTIONS FOR DECISION TREE MODELING  ###
##############################################

# Log Writing
def log_write(log_string):
    f_log = open('trees_log.txt','a')
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    print (timestamp + ': ' + log_string)
    f_log.write(timestamp + ': ' + log_string + '\n')
    f_log.close()



# Determining If Valid Arguments
def arguments_validation(arguments):

    # Specifying Running Mode
    if len(arguments) != 3:
        print "\nthis script needs the arguments {'model' or 'predict'} and {'file_name'} \n"
        sys.exit()
    else:
        if arguments[1] not in ['model','predict']:
            print "\nthis script needs the arguments {'model' or 'predict'} and {'file_name'} \n"
            sys.exit()
        else:
            running_mode = arguments[1]
            file_name = arguments[2]
    return running_mode, file_name



# Formatting Model Data
def make_idx_data_cv(W, revs, word_idx_map, revs_header, running_mode):

    # Formatting Header List To Account For Home And Away Players
    dual_revs_header = []; duals = ['home','away']
    for dual in duals:
        for entry in revs_header:
            dual_revs_header.append(dual + '_' + entry)

    # Iterating Through All Games And Splitting Into Test & Train Sets If Necessary
    train, test = [], []
    for rev in revs:

        # Pulling Player Row Indicies
        x = []; sent = rev["text"]
        words = sent.split()
        for word in words:
            if word in word_idx_map:
                x.append(word_idx_map[word])

        # Iterating Over All Players And Coverting Them To Raw Data
        if max(x) > 0:
            cur_row = []; cur_header = []
            for n in range(0,len(x)):
                cur_row = cur_row + list(W[x[n]])
                cur_header = cur_header + ['p' + str(n) + '_' + element for element in dual_revs_header]

            # Storing Player Fields In Dictionary For Pandas
            cur_dict = {}
            for m in range(0,len(cur_row)):
                cur_dict[cur_header[m]] = cur_row[m]
            cur_dict['y'] = rev["y"]; cur_dict['line'] = str(rev["line"]); cur_dict['game_id'] = rev["game_id"]
            cur_dict['home_team'] = rev["home_team"]; cur_dict['away_team'] = rev["away_team"]

            # If Model Mode, Splitting 1/10 For Train & Test Sets
            if running_mode == 'model':
                if rev["split"] == 1:
                    test.append(cur_dict)
                else:
                    train.append(cur_dict)
            if running_mode == 'predict':
                test.append(cur_dict)

    # Returning Output
    log_write("Number of urls to train: " + str(len(train)))
    log_write("Number of urls to test: " + str(len(test)))
    df_train = pd.DataFrame(train); df_test = pd.DataFrame(test)
    return [df_train, df_test]



# Splitting DataFrames Into Appropriate SubFrames
def split_dataframes(datasets_train, datasets_test, running_mode):

    # Splitting Train Model Into x -> Model Inputs, y -> Model Categorization, g -> Game ID
    if running_mode == 'model':
        x = datasets_train.drop('y',1).drop('game_id',1).drop('line',1).drop('home_team',1).drop('away_team',1)
        y = datasets_train.ix[:,'y']
        g = datasets_train.ix[:,'game_id']
        l = datasets_train.ix[:,'line']
        h = datasets_train.ix[:,'home_team']
        a = datasets_train.ix[:,'away_team']
    if running_mode == 'predict':
        x = []; y = []; g = []; l = []; h = []; a = []

    # Splitting Test Model Into x -> Model Inputs, y -> Model Categorization, g -> Game ID
    x_test = datasets_test.drop('y',1).drop('game_id',1).drop('line',1).drop('home_team',1).drop('away_team',1)
    y_test = datasets_test.ix[:,'y']
    g_test = datasets_test.ix[:,'game_id']
    l_test = datasets_test.ix[:,'line']
    h_test = datasets_test.ix[:,'home_team']
    a_test = datasets_test.ix[:,'away_team']

    # Returning Output
    return x, y, g, l, h, a, x_test, y_test, g_test, l_test, h_test, a_test



# Creating Or Loading Model
def create_load_model(x, y, running_mode):

    # If Model Mode Create Model
    if running_mode == 'model':
        clf = RandomForestClassifier(n_estimators=500)
        clf = clf.fit(x,y)
        print 'Classes: ' + str(clf.classes_)
        with open('trees_model.p','wb') as f:
            cPickle.dump(clf, f)

    # If Predict Mode Loading Model
    if running_mode == 'predict':
        with open('trees_model.p','rb') as f:
            clf = cPickle.load(f)
        print 'Classes: ' + str(clf.classes_)

    # Returning Model
    return clf



# Testing Model On Test / Prediction Set
def test_model(x_test, y_test, g_test, l_test, h_test, a_test, running_mode):

    # Iterating Over All DataFrame Rows
    prediction_list = []
    total = 0.0; successes = 0.0;
    for index, row in x_test.iterrows():

        # Filter Annoying Warning Messages
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            preds = clf.predict_proba(row)

        # Predict best fit category
        cur_dict = {}
        cur_dict['game_id'] = int(g_test[index:index+1])
        cur_dict['line'] = l_test[index:index+1].get(index)
        cur_dict['home_team'] = h_test[index:index+1].get(index)
        cur_dict['away_team'] = a_test[index:index+1].get(index)
        cur_dict['known_category'] = int(y_test[index:index+1])
        if preds[0][1] > 0.5:
            cur_dict['predicted_category_1'] = 1; cur_dict['predicted_p_1'] = preds[0][1]
            cur_dict['predicted_category_2'] = 0; cur_dict['predicted_p_2'] = preds[0][0]
        if preds[0][1] <= 0.5:
            cur_dict['predicted_category_1'] = 0; cur_dict['predicted_p_1'] = preds[0][0]
            cur_dict['predicted_category_2'] = 1; cur_dict['predicted_p_2'] = preds[0][1]
        prediction_list.append(copy.deepcopy(cur_dict))

        # Keeping Track Of Successes
        total = total + 1
        if preds[0][1] > 0.5 and int(y_test[index:index+1]) == 1:
            successes = successes + 1
        if preds[0][1] < 0.5 and int(y_test[index:index+1]) == 0:
            successes = successes + 1

    # Recording Output
    print 'Total: ' + str(total)
    print 'Successes: ' + str(successes)
    print 'Ratio: ' + str(successes / total)

    # Extra Prediction Mode Output
    if running_mode == 'predict':

        # Storing Results File
        f_record = open("trees_model_results.txt","w")
        for element in prediction_list:
            f_record.write(str(element).replace('\n','') + '\n')
        f_record.close()

        # Storing Wagers File
        f_wagers = open('wagers.csv','w')
        f_wagers.write('game_id,home_wins,odds,amount\n')
        for game in prediction_list:
            game_id = str(game['game_id'])
            if int(game['predicted_category_1']) == 1:
                outcome = 'W'
            else:
                outcome = 'L'
            f_wagers.write(game_id + ',' + outcome + ',-110,1\n')
        f_wagers.close()
    return



##############
###  MAIN  ###
##############

if __name__=="__main__":

    # Specifying Running Mode
    running_mode, file_name = arguments_validation(sys.argv)

    # Loading Model Data
    log_write("loading data...")
    x = cPickle.load(open(file_name,"rb"))
    revs, W, word_idx_map, revs_header = x[0], x[1], x[2], x[3]
    log_write("data loaded!")

    # Creating Test And Train Datasets
    datasets = make_idx_data_cv(W, revs, word_idx_map, revs_header, running_mode)

    # Splitting DataFrames Into Appropriate SubFrames
    x, y, g, l, h, a, x_test, y_test, g_test, l_test, h_test, a_test = split_dataframes(datasets[0], datasets[1], running_mode)

    # Creating Or Loading Model
    clf = create_load_model(x, y, running_mode)

    # Testing Model On Test / Prediction Set
    test_model(x_test, y_test, g_test, l_test, h_test, a_test, running_mode)
