# https://blog.csdn.net/qq_41518277/article/details/85101240

# http://www.360doc.com/content/16/0906/01/20558639_588703140.shtml
# https://machinelearningmastery.com/sensitivity-analysis-history-size-forecast-skill-arima-python/

import warnings
warnings.filterwarnings("ignore")

from math import sqrt
from sklearn.metrics import mean_squared_error
from pandas import Series
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA


def anova_analysis():
    from statsmodels.formula.api import ols
    import statsmodels.stats.anova as anova
    # B = ols('Return~C(Industry)', data=A.dropna()).fit()
    # C = anova.anova_lm(B)
    import statsmodels.api as sm
    from statsmodels.formula.api import ols
    moore = sm.datasets.get_rdataset("Moore", "car", cache = True)  # load data
    data = moore.data
    data = data.rename(columns={"partner.status": "partner_status"})  # make name pythonic
    moore_lm = ols('conformity ~ C(fcategory, Sum)*C(partner_status, Sum)', data = data).fit()
    table = sm.stats.anova_lm(moore_lm, typ=2)  # Type 2 ANOVA DataFrame
    print(table)

    stats.f_oneway(data_group1, data_group2, data_group3, data_groupN)


def random_guess(y_arr):
    return np.random.uniform(min(y_arr), max(y_arr), len(y_arr))

def moving_average_guess(y_arr, lag=10):
    return np.random.uniform(min(y_arr), max(y_arr), len(y_arr))

# anova_analysis()
#
# exit(-1)


# load dataset

series = Series.from_csv('./daily-min-temperatures.csv', header=0)
# display first few rows
# print(series.head(20))
# series.plot()
# pyplot.show()

# seasonal difference
differenced = series.diff(365)
# trim off the first year of empty data
differenced = differenced[365:]

# fit model
model = ARIMA(differenced, order=(7, 0, 0))
model_fit = model.fit(trend='nc', disp=0)
print(model_fit.summary())

# split
train, test = differenced[differenced.index < '1990'], differenced['1990']
years = ['1989', '1988', '1987', '1986', '1985', '1984', '1983', '1982']
for year in years:
    print('year is ', year)
    # select data from 'year' cumulative to 1989
    dataset = train[train.index >= year]
    values = dataset.values
    history = [values[i] for i in range(len(values))]
    predictions = list()
    test_values = test.values

    for t in range(len(test_values)):
        # fit model
        model = ARIMA(history, order=(8, 0, 0))
        model_fit = model.fit(trend='nc', disp=0)
        # make prediction
        yhat = model_fit.forecast()[0]
        predictions.append(yhat)
        history.append(test_values[t])

    rmse = sqrt(mean_squared_error(test_values, predictions))
    print('%s-%s (%d values) RMSE: %.3f' % (years[0], year, len(values), rmse))
