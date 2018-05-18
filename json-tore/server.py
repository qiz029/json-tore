#!/usr/bin/env python
# -- coding: utf-8 --
from flask import Flask, request, jsonify
import sys
import json
import socket
from storage import kv_storage as store

reload(sys)
sys.setdefaultencoding('utf8')

app = Flask(__name__)
store = store()

@app.route('/')
def index():
    return "welcome to json-tore, a python based in memory json kv store"

@app.route('/index/<index>', methods=['POST'])
def add_index(index):
    if not request.headers['Content-Type'] == 'application/json':
        return jsonify({"msg": "Only accept json format body"}), 403
    if (store.create_index(index, request.get_json(force=True))):
        return jsonify({"msg": "created index {0}".format(index)}), 201
    return jsonify({"msg": "failed created index {0}".format(index)}), 409

@app.route('/index/<index>', methods=['PUT'])
def update_index(index):
    if not request.headers['Content-Type'] == 'application/json':
        return jsonify({"msg": "Only accept json format body"}), 403
    if (store.update_index(index, request.get_json(force=True))):
        return jsonify({"msg": "created index {0}".format(index)}), 201
    return jsonify({"msg": "failed created index {0}".format(index)}), 409

@app.route('/index/<index>', methods=['GET', 'HEAD'])
def get_index(index):
    if request.method == 'HEAD':
        return check_index(index)
    if not request.headers['Accept'] == 'application/json':
        return jsonify({"msg": "Only is able to return json"}), 403
    msg = store.get_index(index)
    if (msg is None):
        return "", 404
    return jsonify(msg), 200

@app.route('/index/<index>', methods=['DELETE'])
def delete_index(index):
    store.delete_index(index)
    return "", 204

@app.route('/index', methods=['GET'])
def get_all_index():
    return jsonify(store.show_all_index()), 200

@app.route('/size', methods=['GET'])
def get_size():
    return jsonify(store.size()), 200

def check_index(index):
    if store.check_index(index):
        return "", 200
    return "", 404

if __name__ == '__main__':
    app.run(host="localhost", port=80, debug=True)
