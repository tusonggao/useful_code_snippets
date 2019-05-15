import time
import numpy as np
from random import normalvariate  # 正态分布
import matplotlib.pyplot as plt

import lightgbm as lgb
import tensorflow as tf

from tqdm import tqdm_notebook as tqdm

from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from keras.layers import Input, Dense, Dropout, BatchNormalization
from keras.models import Model
from keras.utils import plot_model, to_categorical
from keras.optimizers import Adam
from keras.callbacks import Callback


def rmse_tsg(y_true, y_pred):
    return np.sqrt(np.mean(np.square(y_true - y_pred)))


def showData(X_data, y_data):
    print('X_data.shape is', X_data.shape)
    X_data_pos = X_data[y_data==1]
    X_data_neg = X_data[y_data==0]

    print('X_data_pos.shape is', X_data_pos.shape)
    plt.plot(X_data_pos[:, 200], X_data_pos[:, 800], "ro")
    plt.plot(X_data_neg[:, 200], X_data_neg[:, 800], "bo")
    plt.show()


def gen_data(data_shape, random_seed=None):
    if random_seed is None:
        np.random.seed(int(time.time()))
    else:
        np.random.seed(random_seed)

    lower, upper = -1, 1
    height, width = data_shape
    X_data = np.random.rand(height, width)*(upper - lower) + lower
    y_data = np.zeros(height)

    col1, col2, col3 = 111, 222, 333
    y_data[(X_data[:, col1] - 0.2) * (X_data[:, col2] - 0.2) * (X_data[:, col3] - 0.2) > 0.05] = 1
    y_data[(X_data[:, col1] - 0.2) * (X_data[:, col2] - 0.2) * (X_data[:, col3] - 0.2) < -0.05] = 2

    print('len of y_data[y_data == 0] is ', len(y_data[y_data == 0]),
          'len of y_data[y_data == 1] is ', len(y_data[y_data == 1]),
          'len of y_data[y_data == 2] is ', len(y_data[y_data == 2]))

    return X_data, y_data


def batcher(X_data, y_data, batch_size=-1, random_seed=None):
    if batch_size == -1:
        batch_size = n_samples
    if batch_size < 1:
       raise ValueError('Parameter batch_size={} is unsupported'.format(batch_size))

    if random_seed is None:
        np.random.seed(int(time.time()))
    else:
        np.random.seed(random_seed)

    rnd_idx = np.random.permutation(len(X_data))
    print('rnd_idx[:10] is', rnd_idx[:10])
    n_batches = len(X_data) // batch_size
    for batch_idx in np.array_split(rnd_idx, n_batches):
        X_batch, y_batch = X_data[batch_idx], y_data[batch_idx]
        yield X_batch, y_batch


def keras_DNN_test(X_test, y_test):
    sen = Input(shape=(1000,), dtype='float32', name='input')
    # dense = Dense(2000, activation='selu', kernel_initializer='he_uniform')(sen)
    dense = Dense(2000, activation='relu', kernel_initializer='lecun_normal')(sen)
    dense = BatchNormalization()(dense)

    dense = Dense(1000, activation='selu', kernel_initializer='lecun_normal')(dense)
    dense = BatchNormalization()(dense)

    dense = Dense(200, activation='selu', kernel_initializer='lecun_normal')(dense)
    dense = BatchNormalization()(dense)

    output = Dense(3, activation='softmax', name='output')(dense)
    model = Model(sen, output)

    adam = Adam(lr=0.001)
    # model.compile(loss='mean_squared_error', optimizer=adam)
    # model.compile(optimizer=adam, loss='binary_crossentropy', metrics=['accuracy'])
    model.compile(optimizer=adam, loss='categorical_crossentropy', metrics=['accuracy'])

    plot_model(model, to_file='./model_test.png', show_shapes=True)

    max_epochs = 150
    batch_size = 256

    total_batch_num = 0
    auc_score_best = -999
    test_acc_best = 0.0

    y_test = to_categorical(y_test)


    for epoch in range(max_epochs):
        print('current epoch is ', epoch)
        batch_num = 0
        X_data, y_data = gen_data((10000, 1000))
        for batch_X, batch_y in batcher(X_data, y_data, batch_size):
            batch_num += 1
            total_batch_num += 1
            batch_y = to_categorical(batch_y)
            model.train_on_batch(batch_X, batch_y)
            if total_batch_num % 25 == 0:
                train_loss, train_acc = model.evaluate(batch_X, batch_y, verbose=0)
                test_loss, test_acc = model.evaluate(X_test, y_test, verbose=0)
                if test_acc_best < test_acc:
                    test_acc_best = test_acc
                    start_t = time.time()
                    model.save('./model/test_dnn_model.h5', overwrite=True, include_optimizer=True)
                    print('model saved cost time:', time.time() - start_t,
                          'get new model test_acc_best: ', test_acc_best, )
                print('epoch:', epoch, 'total_batch_num:', total_batch_num,
                      'batch_num:', batch_num, 'train_loss:', train_loss,
                      'train_acc:', train_acc, 'test_loss:', test_loss,
                      'test_acc:', test_acc, 'test_acc_best:', test_acc_best)
        del X_data, y_data



#################################################################################################

if __name__ == "__main__":
    X_val, y_val = gen_data((20000, 1000), random_seed=42)
    keras_DNN_test(X_val, y_val)

