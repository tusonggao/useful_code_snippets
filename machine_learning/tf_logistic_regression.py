import time
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
import lightgbm as lgb

from sklearn.datasets import fetch_california_housing
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split


def sigmoid(z):
    return 1 / (1 + np.exp( - z))


def create_mock_data():
    # np.random.seed(42)
    W = np.array([1.1, 2.2, -3.3, 4.4])
    b = 0.77
    X = np.random.rand(1000, 4)
    y = np.dot(X, W) + b
    y = np.where(y>3.3, 1., 0.)
    print('X.shape is ', X.shape, y.shape, y.sum())
    return X, y

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
    X_merged = StandardScaler().fit_transform(X_merged)        # 标准转化
    # X_merged = np.c_[np.ones((len(X_merged), 1)), X_merged]  # 增加一列1，用于学习bias数值
    X_train = X_merged[:len(X_train)]
    X_test = X_merged[len(X_train):]
    y_train = y_train.reshape(-1, 1)
    y_test = y_test.reshape(-1, 1)

    X = tf.placeholder(dtype=tf.float32, shape=(None, X_train.shape[1]), name='X')
    y = tf.placeholder(dtype=tf.float32, shape=(None, 1), name="y")
    W = tf.Variable(tf.random_uniform([X_train.shape[1], 1], -1.0, 1.0), name='weights')
    b = tf.Variable(0, dtype=tf.float32, name='bias')
    # y_pred = tf.add(tf.matmul(X, W) + b, name="predictions")
    y_pred = tf.sigmoid(tf.add(tf.matmul(X, W), b), name='predictions')

    optimizer = tf.train.GradientDescentOptimizer(learning_rate=learning_rate)
    # optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.9)
    loss = tf.nn.sigmoid_cross_entropy_with_logits(labels=y, logits=y_pred)
    training_op = optimizer.minimize(loss)

    loss_history, accuracy_history = [], []
    init = tf.global_variables_initializer()
    with tf.Session() as sess:
        sess.run(init)
        for epoch in range(n_epochs):
            print('epoch is ', epoch)
            for X_batch, y_batch in batcher(X_train, y_train, batch_size):
                sess.run(training_op, feed_dict={X: X_batch, y: y_batch})

            y_class_pred = np.where(y_pred.eval({X: X_test, y: y_test})>0.5, 1, 0)
            correct_prediction = tf.equal(y_class_pred, y_test)
            accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

            # Storing Loss and Accuracy to the history
            current_loss = sum(sum(sess.run(loss, feed_dict={X: X_test, y: y_test})))
            current_acc = sess.run(accuracy, feed_dict={X: X_test, y: y_test}) * 100
            loss_history.append(current_loss)
            accuracy_history.append(current_acc)
            print('epoch is ', epoch, 'current_loss is ', current_loss,
                  'current_acc is', current_acc)

            run_epochs = epoch
            if current_loss<=25:
                break


        # if epoch % 10 == 0:
        #     mse_val = sess.run(mse, feed_dict={X: X_test, y: y_test})
        #     print("Epoch", epoch, "MSE =", mse_val)
        best_W = W.eval()
        best_bias = b.eval()
        print('best_W is ', best_W, 'best_bias is ', best_bias)

        plt.plot(list(range(run_epochs+1)), loss_history)
        # plt.plot(list(range(epochs)), accuracy_history)
        plt.xlabel('Epochs')
        plt.ylabel('Cost')
        plt.title('Decrease in Cost with Epochs')
        plt.show()


if __name__=='__main__':
    X_data, y_data = create_mock_data()
    # housing = fetch_california_housing()
    # X_data = housing.data
    # y_data = housing.target
    #
    X_train, X_test, y_train, y_test = train_test_split(X_data, y_data, test_size=0.3, random_state=42)
    print('X_train.shape is', X_train.shape, 'y_train.shape is', y_train.shape)
    print('X_test.shape is', X_test.shape, 'y_test.shape is', y_test.shape)
    logistic_regression_tf(X_train, y_train, X_test, y_test, n_epochs=3000, batch_size=16)
