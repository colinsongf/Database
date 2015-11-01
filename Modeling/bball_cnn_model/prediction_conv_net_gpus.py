"""
Sample code for
Convolutional Neural Networks for Sentence Classification
http://arxiv.org/pdf/1408.5882v2.pdf

Much of the code is modified from
- deeplearning.net (for ConvNet classes)
- https://github.com/mdenil/dropout (for dropout)
- https://groups.google.com/forum/#!topic/pylearn-dev/3QbKtCumAW4 (for Adadelta)
"""
import cPickle
import numpy as np
from psycopg2.extras import DictCursor
import psycopg2
from collections import defaultdict, OrderedDict
import theano
from theano import function, config, shared, sandbox
import theano.sandbox.cuda.basic_ops
import theano.tensor as T
from theano import config, shared, sandbox
import numpy
import re
import warnings
import sys
import time
import datetime
from pprint import pprint
warnings.filterwarnings("ignore")

def ReLU(x):
    y = T.maximum(0.0, x)
    return(y)
def Sigmoid(x):
    y = T.nnet.sigmoid(x)
    return(y)
def Tanh(x):
    y = T.tanh(x)
    return(y)
def Iden(x):
    y = x
    return(y)

def as_floatX(variable):
    if isinstance(variable, float):
        return np.cast[theano.config.floatX](variable)

    if isinstance(variable, np.ndarray):
        return np.cast[theano.config.floatX](variable)
    return theano.tensor.cast(variable, theano.config.floatX)

def safe_update(dict_to, dict_from):
    """
    re-make update dictionary for safe updating
    """
    for key, val in dict(dict_from).iteritems():
        if key in dict_to:
            raise KeyError(key)
        dict_to[key] = val
    return dict_to

def get_idx_from_sent(sent, word_idx_map, max_l=51, k=300, filter_h=5):
    """
    Transforms sentence into a list of indices. Pad with zeroes.
    """
    x = []
    pad = filter_h - 1
    for i in xrange(pad):
        x.append(0)
    words = sent.split()
    for word in words:
        if word in word_idx_map:
            x.append(word_idx_map[word])
    while len(x) < max_l+2*pad:
        x.append(0)
    return x

def make_idx_data_cv(revs, word_idx_map, max_l=51, k=300, filter_h=5):
    """
    Transforms sentences into a 2-d matrix.
    """
    prediction_list = []
    prediction_counter = 0
    train, test = [], []
    for rev in revs:
        sent = get_idx_from_sent(rev["text"], word_idx_map, max_l, k, filter_h)
        if max(sent) > 0:
            test.append(sent)
            prediction_list.append({})
            prediction_list[prediction_counter]["text"] = rev["text"]
            prediction_list[prediction_counter]["known_category"] = rev["y"]
            prediction_counter = prediction_counter + 1
    print "Number of urls to predict: " + str(len(test))
    test = np.array(test,dtype="int")
    return [test,prediction_list]

if __name__=="__main__":

    print "loading data..."
    x = cPickle.load(open("mr_prediction.p","rb"))
    revs, W, word_idx_map = x[0], x[1], x[2]
    U = W.astype(dtype="float32")
    datasets = make_idx_data_cv(revs, word_idx_map, max_l=56,k=40, filter_h=5)
    execfile("conv_net_classes.py")

    print "loading model..."
    x = cPickle.load(open("cnn_model.pickle","rb"))
    W_params = x[0].get_value()
    b_params = x[1].get_value()
    W_conv0_params = x[2].get_value()
    b_conv0_params = x[3].get_value()
    W_conv1_params = x[4].get_value()
    b_conv1_params = x[5].get_value()
    W_conv2_params = x[6].get_value()
    b_conv2_params = x[7].get_value()

    img_w=40
    lr_decay=0.95
    filter_hs=[2,3,4,5]
    conv_non_linear="relu"
    hidden_units=[100,2] #903
    shuffle_batch=True
    sqr_norm_lim=9
    batch_size=50
    dropout_rate=[0.5]
    non_static=True
    activations=[Iden]
    relaxation = 1

    y = T.ivector('y')
    rng = np.random.RandomState(3435)
    img_h = len(datasets[0][0])-1
    filter_w = img_w
    feature_maps = hidden_units[0]
    filter_shapes = []
    pool_sizes = []
    for filter_h in filter_hs:
        filter_shapes.append((feature_maps, 1, filter_h, filter_w))
        pool_sizes.append((img_h-filter_h+1, img_w-filter_w+1))
    parameters = [("image shape",img_h,img_w),("filter shape",filter_shapes), ("hidden_units",hidden_units),
                  ("dropout", dropout_rate), ("batch_size",batch_size),("non_static", non_static),
                    ("learn_decay",lr_decay), ("conv_non_linear", conv_non_linear), ("non_static", non_static)
                    ,("sqr_norm_lim",sqr_norm_lim),("shuffle_batch",shuffle_batch)]

    #define model architecture
    index = T.lscalar()
    x = T.matrix('x')
    y = T.ivector('y')
    Words = theano.shared(value = U, name = "Words")
    zero_vec_tensor = T.vector()
    zero_vec = np.asarray(np.zeros(img_w),"float32")
    set_zero = theano.function([zero_vec_tensor], updates=[(Words, T.set_subtensor(Words[0,:], zero_vec_tensor))]) #, allow_input_downcast=True
    layer0_input = Words[T.cast(x.flatten(),dtype="int32")].reshape((x.shape[0],1,x.shape[1],Words.shape[1]))
    conv_layers = []
    layer1_inputs = []
    for i in xrange(len(filter_hs)):
        filter_shape = filter_shapes[i]
        pool_size = pool_sizes[i]
        conv_layer = LeNetConvPoolLayer(rng, input=layer0_input, lnum=i, image_shape=(batch_size, 1, img_h, img_w),
                                filter_shape=filter_shape, poolsize=pool_size, non_linear=conv_non_linear)
        layer1_input = conv_layer.output.flatten(2)
        conv_layers.append(conv_layer)
        layer1_inputs.append(layer1_input)
    layer1_input = T.concatenate(layer1_inputs,1)
    hidden_units[0] = feature_maps*len(filter_hs)
    classifier = MLPDropout(rng, input=layer1_input, layer_sizes=hidden_units, activations=activations, dropout_rates=dropout_rate)

    #setting classifier parameters
    params = classifier.params
    for conv_layer in conv_layers:
        params += conv_layer.params
    params[0].set_value(W_params)
    params[1].set_value(b_params)
    params[2].set_value(W_conv0_params)
    params[3].set_value(b_conv0_params)
    params[4].set_value(W_conv1_params)
    params[5].set_value(b_conv1_params)
    params[6].set_value(W_conv2_params)
    params[7].set_value(b_conv2_params)

    #making predictions
    print "making predictions..."
    test_set_x = np.asarray(datasets[0][:,:img_h],"float32")
    pred_batch_size = 100
    test_data_size = test_set_x.shape[0]
    print "batch size: " + str(pred_batch_size)
    print "data size: " + str(test_data_size)
    test_batches = int(test_data_size) / int(pred_batch_size) + 1
    for i in range(0,test_batches):

        bottom_index = i * pred_batch_size
        top_index = min(test_data_size,(i+1)*pred_batch_size)
        cur_test_set = test_set_x[bottom_index:top_index]

        test_pred_layers = []
        test_size = cur_test_set.shape[0]
        if test_size == 0:
            break
        test_layer0_input = Words[T.cast(x.flatten(),dtype="int32")].reshape((test_size,1,img_h,Words.shape[1]))
        for conv_layer in conv_layers:
            test_layer0_output = conv_layer.predict(test_layer0_input, test_size)
            test_pred_layers.append(test_layer0_output.flatten(2))
        test_layer1_input = T.concatenate(test_pred_layers, 1)

        # probability mode
        test_y_pred = classifier.predict_p(test_layer1_input)
        test_output_fn = theano.function([x], sandbox.cuda.basic_ops.gpu_from_host(T.cast(test_y_pred,'float32')))
        new_preds = np.asarray(test_output_fn(cur_test_set))
        new_preds_dict = {}
        new_preds_p_dict = {}
        for w in range(1,relaxation + 1):
            new_preds_dict[w] = []
            new_preds_p_dict[w] = []
        for cur_pred in new_preds:
            sorted_cur_pred = np.sort(cur_pred)
            sorted_len = len(sorted_cur_pred)
            for w in range(1,relaxation + 1):
                max_cur = sorted_cur_pred[sorted_len - w]
                max_cur_index = np.where(cur_pred == max_cur)[0][0]
                new_preds_dict[w].append(max_cur_index);
                new_preds_p_dict[w].append(max_cur)

        if i == 0:
            print "batch shape: " + str(cur_test_set.shape)
            predictions_dict = {}
            predictions_p_dict = {}
            for w in range(1,relaxation + 1):
                predictions_dict[w] = np.asarray(new_preds_dict[w]);
                predictions_p_dict[w] = np.asarray(new_preds_p_dict[w])
        else:
            print "predictions made: " + str(bottom_index)
            for w in range(1,relaxation + 1):
                predictions_dict[w] = np.concatenate((predictions_dict[w],new_preds_dict[w]), axis=0);
                predictions_p_dict[w] = np.concatenate((predictions_p_dict[w],new_preds_p_dict[w]), axis=0)

    # organizing predictions
    prediction_list = datasets[1]
    for i in range(0,len(predictions_dict[1])):
        for w in range(1,relaxation + 1):
            prediction_list[i]["predicted_category_" + str(w)] = predictions_dict[w][i]
            prediction_list[i]["predicted_p_" + str(w)] = predictions_p_dict[w][i]
    if len(prediction_list) == len(predictions_dict[1]):
        print "all predictions made"
    else:
        print "some predictions missed"

    #formatting output
    print "formatting output..."
    count_right_dict = {}
    for w in range(1,relaxation + 1):
        count_right_dict[w] = 0
    count_total = 0
    pair_misses = {}
    for entry in prediction_list:
        count_total = count_total + 1
        cur_lvl = 1
        while cur_lvl <= relaxation:
            if int(entry["predicted_category_" + str(cur_lvl)]) == int(entry["known_category"]):
                for w in range(cur_lvl,relaxation + 1):
                    count_right_dict[w] = count_right_dict[w] + 1
                if cur_lvl == relaxation:
                    pair = str(entry["predicted_category_1"]) + ':' + str(entry["known_category"])
                    if pair not in pair_misses:
                        pair_misses[pair] = 0
                    pair_misses[pair] = pair_misses[pair] + 1
                break
            else:
                cur_lvl = cur_lvl + 1

    print "count total: " + str(count_total)
    for w in range(1,relaxation + 1):
        suffix_str = ''
        for u in range(1,w + 1):
            suffix_str = suffix_str + str(u) + ' '
        print ''
        print "count right " + suffix_str + ": " + str(count_right_dict[w])
        print "proportion " + suffix_str + ": " + str(float(count_right_dict[w]) / float(count_total))
        print ''

    f_record = open("prediction_results","w")
    for element in prediction_list:
        f_record.write(str(element).replace('\n','') + '\n')
    f_record.close()
