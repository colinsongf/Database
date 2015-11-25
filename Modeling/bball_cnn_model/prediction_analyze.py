from psycopg2.extras import DictCursor
import psycopg2
import numpy as np
import cPickle
from collections import defaultdict
import sys, re
import pandas as pd
from pprint import pprint
import operator
import copy
import ast
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches



#####################
###   FUNCTIONS   ###
#####################

# creating the dictionary tracking counts and success for each bucket
def create_buckets(spacing,relaxation):
    results = {}
    upper_bound = 100 / spacing
    for i in range(0,upper_bound):
        results[i*spacing + spacing] = {}
        results[i*spacing + spacing]["total"] = 0.0
        for w in range(1,relaxation + 1):
            results[i*spacing + spacing]["right_" + str(w)] = 0.0
    return results


# determining which bucket a sample game falls into
def get_bucket(spacing,sample,mode):
    bucket_point = float(sample[mode])*100
    if bucket_point % spacing == 0:
        bucket = int(bucket_point)
    else:
        bucket = int(bucket_point + (spacing - bucket_point % spacing))
    return bucket


# placing all game predictions into buckets
def bucketing_predictions(spacing,relaxation,results,results_file):

    # iterating over all games
    games_detailed = {}
    f = open(results_file,"r")
    for line in f:
        sample = ast.literal_eval(line.replace('\n',''))
        games_detailed[sample["game_id"]] = {}
        games_detailed[sample["game_id"]]["predicted_category_1"] = sample["predicted_category_1"]
        games_detailed[sample["game_id"]]["known_category"] = sample["known_category"]
        games_detailed[sample["game_id"]]["predicted_p_1"] = sample["predicted_p_1"]

        # for a game, placing prediction into a bucket for all relaxation levels
        bucket_dict = {}
        for w in range(1,relaxation + 1):
            bucket_dict[w] = get_bucket(spacing,sample,"predicted_p_" + str(w))
            if w == 1:
                games_detailed[sample["game_id"]]["bucket"] = bucket_dict[w]
        results[bucket_dict[1]]["total"] = results[bucket_dict[1]]["total"] + 1.0

        # additionally placing prediction success into a bucket if available
        cur_lvl = 1
        while cur_lvl <= relaxation:
            if sample["predicted_category_" + str(cur_lvl)] == sample["known_category"]:
                results[bucket_dict[1]]["right_" + str(cur_lvl)] = results[bucket_dict[1]]["right_" + str(cur_lvl)] + 1.0
                break
            else:
                cur_lvl = cur_lvl + 1

    # returning bucketed predictions
    f.close()
    return results, games_detailed


# determining bucket prediction accuracies
def bucket_prediction_accuracies(relaxation,results):

    # prepping data structures
    x_dict = {}; y_dict = {}; z_dict = {}
    for w in range(1,relaxation + 1):
        x_dict[w] = []; y_dict[w] = []; z_dict[w] = []
    q1 = []; entries = []

    # iterating over each bucket
    for entry in results:

        # for each level of relaxation, calculating # of successes for <= current relaxation
        cumulative_sums_dict = {}
        for w in range(1,relaxation + 1):
            cumulative_sums_dict[w] = 0
            for u in range(1,w + 1):
                cumulative_sums_dict[w] = cumulative_sums_dict[w] + results[entry]["right_" + str(u)]

        # for each level of relaxation, storing bucket -> x , prediction_accuracy -> y , success_count -> z
        for w in range(1,relaxation + 1):
            x_dict[w].append(entry)
            y_append_val = (cumulative_sums_dict[w] / results[entry]["total"]) if results[entry]["total"] > 0 else 0.0
            y_dict[w].append(y_append_val)
            z_dict[w].append(cumulative_sums_dict[w])

        # storing total count of games within bucket
        q1.append(results[entry]["total"])

        # storing bucket order
        entries.append(entry)

    # return data needed for plotting
    return x_dict, y_dict, z_dict, q1, entries


# ordering x,y,z tuples with respect to x for plotting
def sorting_data(relaxation,x_dict,y_dict,z_dict,q1,entries):
    for w in range(1,relaxation + 1):
        x = x_dict[w]; y = y_dict[w]; z = z_dict[w]
        sorting = zip(*sorted(zip(y,z,x), key=operator.itemgetter(2)))
        x_dict[w] = sorting[2]; y_dict[w] = sorting[0]; z_dict[w] = sorting[1]
    q1 = zip(*sorted(zip(q1,entries), key=operator.itemgetter(1)))[0]
    return x_dict, y_dict, z_dict, q1, entries


# removing buckets with 0 data points
def truncating_buckets(relaxation,x_dict,y_dict,z_dict,q1):

    # creating new dictionary templates
    x_dict_new = {}; y_dict_new = {}; z_dict_new = {}
    for w in range(1,relaxation + 1):
        x_dict_new[w] = []; y_dict_new[w] = []; z_dict_new[w] = []
    q1_new = []

    # truncating old dictionaries into new dictionaries
    for w in range(1,relaxation + 1):
        for j in range(0,len(x_dict[w])):
            if q1[j] > 0:
                x_dict_new[w].append(x_dict[w][j])
                y_dict_new[w].append(y_dict[w][j])
                z_dict_new[w].append(z_dict[w][j])
                if w == relaxation:
                    q1_new.append(q1[j])

    # returning new dictionaries
    return x_dict_new, y_dict_new, z_dict_new, q1_new


# plotting of prediction accuracy and bin quantities
def mode_1_plotting(relaxation,x_dict,y_dict,z_dict,q1):

    f = plt.figure(1)
    handles_list = []
    for w in range(1,relaxation + 1):
        plt.scatter(np.array(x_dict[w]),np.array(y_dict[w]),c=c_dict[w],s=50)
        plt.plot(np.array(x_dict[w]),np.array(y_dict[w]),'-',c=c_dict[w])
        cur_handle = mpatches.Patch(color=c_dict[w],label='relax ' + str(w))
        handles_list.append(cur_handle)
    plt.title('Prediction Accuracy For Expected Probability Bins')
    plt.legend(handles=handles_list,bbox_to_anchor=(0.4,0.8))
    plt.grid('on')

    g = plt.figure(2)
    handles_list = []
    for w in range(1,relaxation + 1):
        plt.scatter(np.array(x_dict[w]),np.array(z_dict[w]),c=c_dict[w],s=50)
        plt.plot(np.array(x_dict[w]),np.array(z_dict[w]),'-',c=c_dict[w])
        plt.scatter(np.array(x_dict[w]),np.array(q1),c='black',s=50)
        cur_handle = mpatches.Patch(color=c_dict[w],label='relax ' + str(w))
        handles_list.append(cur_handle)
    q_handle = mpatches.Patch(color='black',label='total')
    handles_list.append(q_handle)
    plt.title('Quantities Predicted For Expected Probability Bins')
    plt.legend(handles=handles_list,bbox_to_anchor=(0.9,0.9))
    plt.grid('on')

    for w in range(1,relaxation + 1):
        print str(sum(z_dict[w]) / sum(q1)) + ' : ' + str(sum(z_dict[w]))
    plt.show()



################
###   MAIN   ###
################

# initial parameters
bucket_spacing = 4; relaxation = 2; win_gains = 100; lose_losses = 110;
c_dict = ['yellow','blue','red','green','orange','purple','seagreen','cyan','skyblue','coral','crimson']

# ensuring input file
if len(sys.argv) != 2:
    print "\nthis script needs the argument {'file_name'}\n"; sys.exit()
results_file = sys.argv[1]

# creating the dictionary tracking counts and success for each bucket
results = create_buckets(bucket_spacing,relaxation)

# placing all game predictions into buckets
results, games_detailed = bucketing_predictions(bucket_spacing,relaxation,results,results_file)

# determining bucket prediction accuracies
x_dict, y_dict, z_dict, q1, entries = bucket_prediction_accuracies(relaxation,results)

# ordering x,y,z tuples with respect to x for plotting
x_dict, y_dict, z_dict, q1, entries = sorting_data(relaxation,x_dict,y_dict,z_dict,q1,entries)

# removing buckets with 0 data points
x_dict, y_dict, z_dict, q1 = truncating_buckets(relaxation,x_dict,y_dict,z_dict,q1)

# plotting output
mode_1_plotting(relaxation,x_dict,y_dict,z_dict,q1)
