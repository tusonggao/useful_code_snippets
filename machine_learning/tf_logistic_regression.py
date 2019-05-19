import time
import numpy as np
import tensorflow as tf
import lightgbm as lgb

from sklearn.datasets import fetch_california_housing
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

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
    n_batches = len(X_data) // batch_size
    for batch_idx in np.array_split(rnd_idx, n_batches):
        X_batch, y_batch = X_data[batch_idx], y_data[batch_idx]
        yield X_batch, y_batch


def logistic_regression_tf(X_train, y_train, X_test, y_test, n_epochs=100, batch_size=128, learning_rate=0.01):
    X_merged = np.r_[X_train, X_test]
    X_merged = StandardScaler().fit_transform(X_merged)      # 标准转化
    X_merged = np.c_[np.ones((len(X_merged), 1)), X_merged]  # 增加一列1，用于学习bias数值
    X_train = X_merged[:len(X_train)]
    X_test = X_merged[len(X_train):]
    y_train = y_train.reshape(-1, 1)
    y_test = y_test.reshape(-1, 1)

    X = tf.placeholder(dtype=tf.float32, shape=(None, X_train.shape[1]), name='X')
    y = tf.placeholder(dtype=tf.float32, shape=(None, 1), name="y")
    theta = tf.Variable(tf.random_uniform([X_train.shape[1], 1], -1.0, 1.0), name="theta")
    y_pred = tf.matmul(X, theta, name="predictions")
    error = y_pred - y
    mse = tf.reduce_mean(tf.square(error), name="mse")
    optimizer = tf.train.GradientDescentOptimizer(learning_rate=learning_rate)
    # optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.9)

    alpha = 0.003
    loss = tf.add(mse, alpha*tf.nn.l2_loss(theta))
    # loss = tf.add(mse, tf.contrib.layers.l1_regularizer(0.01)(theta))
    # loss = mse
    training_op = optimizer.minimize(loss)

    init = tf.global_variables_initializer()
    with tf.Session() as sess:
        sess.run(init)
        mse_val = sess.run(mse, feed_dict={X: X_test, y: y_test})
        print("first MSE =", mse_val)
        for epoch in range(n_epochs):
            # print('epoch is ', epoch)
            for X_batch, y_batch in batcher(X_train, y_train, batch_size):
                sess.run(training_op, feed_dict={X: X_batch, y: y_batch})
            if epoch % 10 == 0:
                mse_val = sess.run(mse, feed_dict={X: X_test, y: y_test})
                print("Epoch", epoch, "MSE =", mse_val)
        best_theta = theta.eval()
        print('best_theta is ', best_theta)
        mse_val = sess.run(mse, feed_dict={X: X_test, y: y_test})
        print("final MSE =", mse_val)


if __name__=='__main__':
    housing = fetch_california_housing()
    X_data = housing.data
    y_data = housing.target

    X_train, X_test, y_train, y_test = train_test_split(X_data, y_data, test_size=0.3, random_state=42)
    print('X_train.shape is', X_train.shape, 'y_train.shape is', y_train.shape)
    print('X_test.shape is', X_test.shape, 'y_test.shape is', y_test.shape)
    linear_regression_tf(X_train, y_train, X_test, y_test)
