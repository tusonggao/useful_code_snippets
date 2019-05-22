from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
import pickle
import pandas as pd

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'OSPA',
        'description': u'This is ospaf-api test',
        'done': False
    },
    {
        'id': 2,
        'title': u'Garvin',
        'description': u'I am garvin',
        'done': False
    }
]

@app.route('/', methods=['GET'])
def home():
    return 'Hello World!'


@app.route('/get_json/', methods=['GET'])
def oop():
    return jsonify({'tasks': tasks})


@app.route('/add/<num1>/<num2>', methods=['GET'])
def add(num1, num2):
    d = {"num1": num1, "num2": num2}
    print('in add, d is ', d)
    return str(int(num1)+int(num2))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)