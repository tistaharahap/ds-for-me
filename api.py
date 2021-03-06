# -*- coding: utf-8 -*-
from flask import Flask, current_app, render_template
import redis
import time
import json


REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

app = Flask(__name__)
app.debug = True
app.redis = None
app.ds = {
    'name': 'ds-articles-ok'
}

@app.before_first_request
def first():
    current_app.redis = redis.StrictRedis(host=REDIS_HOST,
                                          port=REDIS_PORT)

@app.route('/', methods=['GET'])
def welcome():
    now = int(time.time())
    items = current_app.redis.zrange(current_app.ds['name'], 0, now)

    return render_template('index.html', 
                           keys=[json.loads(item) for item in items])

if __name__ == '__main__':
    app.run()