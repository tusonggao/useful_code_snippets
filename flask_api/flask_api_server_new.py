# 参考链接：
# https://www.jianshu.com/p/f3624eebff80
# http://blog.luisrei.com/articles/flaskrest.html

from flask import Flask, jsonify, request, Response
import pickle
import pandas as pd
import json

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

@app.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404

    return resp

@app.route('/')
def api_root():
    return 'Welcome'

@app.route('/articles')
def api_articles():
    return 'List of ' + url_for('api_articles')

@app.route('/articles/<articleid>')
def api_article(articleid):
    return 'You are reading ' + articleid


@app.route('/', methods=['GET'])
def home():
    return 'Hello World!'


@app.route('/get_json/', methods=['GET'])
def get_json():
    return jsonify({'tasks': tasks})


@app.route('/add/<num1>/<num2>', methods=['GET'])
def add(num1, num2):
    d = {"num1": num1, "num2": num2}
    print('in add, d is ', d)
    return str(int(num1)+int(num2))


@app.route('/sub/<num1>/<num2>', methods=['GET'])
def sub(num1, num2):
    d = {"num1": num1, "num2": num2}
    print('in add, d is ', d)
    return str(int(num1)-int(num2))


@app.route('/multiply/<num1>/<num2>', methods=['GET'])
def multiply(num1, num2):
    d = {"num1": num1, "num2": num2}
    print('in add, d is ', d)
    return str(int(num1)*int(num2))

@app.route('/divide/<num1>/<num2>', methods=['GET'])
def divide(num1, num2):
    d = {"num1": num1, "num2": num2}
    print('in add, d is ', d)
    return str(int(num1)/int(num2))

@app.route('/echo', methods = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE'])
def echo():
    if request.method == 'GET':
        return "ECHO: GET\n"
    elif request.method == 'POST':
        return "ECHO: POST\n"
    elif request.method == 'PATCH':
        return "ECHO: PACTH\n"
    elif request.method == 'PUT':
        return "ECHO: PUT\n"
    elif request.method == 'DELETE':
        return "ECHO: DELETE"


@app.route('/messages', methods = ['POST'])
def api_message():
    if request.headers['Content-Type'] == 'text/plain':
        return "Text Message: " + request.data
    elif request.headers['Content-Type'] == 'application/json':
        return "JSON Message: " + json.dumps(request.json)
    elif request.headers['Content-Type'] == 'application/octet-stream':
        f = open('./binary', 'wb')
        f.write(request.data)
        f.close()
        return "Binary message written!"
    else:
        return "415 Unsupported Media Type ;)"



@app.route('/hello', methods = ['GET'])
def api_hello():
    data = {
        'hello'  : 'world',
        'number' : 3
    }
    js = json.dumps(data)
    resp = Response(js, status=200, mimetype='application/json')
    resp.headers['Link'] = 'http://tusonggao.com'
    return resp


@app.route('/test_args')
def api_test_args():
    if 'name' in request.args:
        name = request.args['name']
    else:
        name = 'Andy'

    if 'age' in request.args:
        age = request.args['age']
    else:
        age = 20

    return 'Hello {}, you are {} years old!'.format(name, age)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True) # 这种方法可以支持对外访问
    # app.run(host='127.0.0.1', port=8080, debug=True)  # 这种方法本机可以访问，但对外访问可能会有问题
    # app.run(host='localhost', port=8080, debug=True)  # 这种方法本机可以访问，但对外访问可能会有问题

