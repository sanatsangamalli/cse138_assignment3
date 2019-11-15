# CSE 138 Assignment 3
from flask import Flask, request, render_template, jsonify
import os
import sys
from mainKeyVal import mainKeyVal

app = Flask(__name__)

@app.route("/")
def begin():
	return "welcome to the home page"

@app.route("/kv-store/keys/<string:key_name>", methods = ["GET","PUT", "DELETE"])
def keyValStore(key_name):
	if request.method == "PUT":
		return server.put(request, key_name)
	elif request.method == "GET":
		return server.get(request, key_name)
	elif request.method == "DELETE":
		return server.delete(request, key_name)

@app.route("/kv-store/key-count", methods = ["GET"])
def keyCount():
	if request.method == "GET":
		return server.getKeyCount()


@app.route("/kv-store/view-change", methods = ["PUT", "prime",  "startChange", "receiveValue", "doneAck"])
def view_change():
	if request.method == "PUT":
		return server.viewChange(request)

@app.route("/kv-store/view-change/receive", methods = ["PUT", "GET", "POST"])
def receive():
	if request.method == "PUT":
		arguments = request.args.to_dict()
		k = arguments["key"]
		v - arguments["value"]
		a = request.remote_addr
		return server.receiveValue(k,v,a)
	elif request.method == "GET":
		arguments = request.args.to_dict()
		print("got get request about to prime")
		return server.prime(request.host, arguments["view"])
	elif request.method == "POST":
		arguments = request.args.to_dict()
		count = int(arguments["count"])
		print('count is:')
		print(count)
		return server.startChange(count)

#@app.route("/kv-store/key-count", methods = ["GET"])
#def countKey(key_name):
#	if request.method == "GET":
#		return server.put(request, key_name)


#@app.route("/kv-store/view-change", methods = ["PUT"])
#def changeView(key_name):
#	if request.method == "PUT":
#		return server.put(request, key_name)

if __name__ == "__main__":
	if 'VIEW' in os.environ:
		server = mainKeyVal(os.environ['VIEW'])
	app.run(debug=True, host = '0.0.0.0', port = 13800, threaded = True)
