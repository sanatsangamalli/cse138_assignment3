from flask import Flask, request, render_template, jsonify, Response
import os
import sys
import threading
from multiprocessing.dummy import Pool as ThreadPool
import requests
import json
import operator


class mainKeyVal:
	def __init__(self, myView):
		self.changingView = False 
		self.dictionary = {}
		self.view = myView.split(',')

		self.leadingViewChange = False
		self.receiveFinalMessageEvent = threading.Event()
		# self.address = os.

	def get(self, request, key_name):
		if key_name in self.dictionary:
			return jsonify({"doesExist":True, "message":"Retrieved successfully", "value":self.dictionary[key_name]}), 200
		else:
			return jsonify({"doesExist":False, "error:":"Key does not exist", "message":"Error in GET"}), 404
		
	def put(self, request, key_name):
		if len(key_name) > 50:
			return jsonify({"error:":"Key is too long", "message":"Error in PUT"}), 400
		req_data = request.get_json(silent=True)
		if req_data is not None and 'value' in req_data:
			data = req_data['value']
			replaced = key_name in self.dictionary
			self.dictionary[key_name] = data	
			
			if replaced:
				message = "Updated successfully"
				code = 200
			else:
				message = "Added successfully"
				code = 201
				
			return jsonify({"message":message, "replaced":replaced}), code
			
		else:
			return jsonify({"error:":"Value is missing", "message":"Error in PUT"}), 400
			
	def delete(self, request, key_name):
		if key_name in self.dictionary:
			del self.dictionary[key_name]
			return jsonify({"doesExist":True, "message":"Deleted successfully"}), 200
		else:
			return jsonify({"doesExist":False, "error:":"Key does not exist", "message":"Error in DELETE"}), 404
	

	# arbiter 
	# request.get_json 
	def viewChange(self, request):
		# send prime message with view to other ip address in view 
		self.changingView = True
		self.leadingViewChange = True
		req_data = request.get_json(silent=True)
		newView = req_data["view"]
		view = newView.split(',')

		receivers = view.copy()
		receivers.remove(request.host)

		pool = ThreadPool(len(receivers))
		resultingMsgVectors = pool.map(self.sendPrimeMessage, receivers)
		pool.close()

		myMsgVector = self.prime(request.host, newView)

		pool.join()

		self.totalMsgVector = myMsgVector
		print(type(self.totalMsgVector))
		print(self.totalMsgVector[0])
		self.totalMsgVector = (self.totalMsgVector[0].get_json())["messageVector"]
		print('here')
		print(type(self.totalMsgVector))
		print(self.totalMsgVector)
		# add message vector to our running total
		for response in resultingMsgVectors:
			print('test message ')
			# vectorResponse =  response.get_json()['messageVector']
			print(type(response))
			print(response)
			vectorResponse =  response.json()["messageVector"]
			print(vectorResponse)
			print(type(vectorResponse))
			# print(json.loads(vectorResponse)["messageVector"])
			self.totalMsgVector = tuple(map(operator.add, (self.totalMsgVector), vectorResponse))#json.loads(vectorResponse)["messageVector"
			# self.totalMsgVector = self.totalMsgVector + json.loads(response.json())['vector']

		self.receiveFinalMsg = threading.Event()	
		self.finalMsgReceive = False  
		# if we've received the last one, send start message with final total
		# each element should be of format i.e. { "address": "10.10.0.2:13800", "key-count": 5 },
		shardPool = ThreadPool(len(receivers))
		shards = shardPool.map(self.sendStartMessage, receivers)
		shardPool.close()

		hostShard = self.startChange(self.totalMsgVector)

		shardPool.join()
		

		print('shard')
		for shard in shards:
			print(type(shard))
			print(shard)
			shard = {"address": shard.json()["address"], "key-count": shard.json()["key-count"]}
		shards.append(hostShard)


		return jsonify({"message": "View change successful", "shards": shards}), 200 
		# self.completedViewChange = threading.Event()
		# self.completedViewChange.wait()



	def sendPrimeMessage(self, address):
		return requests.get('http://'+ address + '/kv-store/view-change/receive?view='+ (",".join(self.view)),timeout=20)

	def sendStartMessage(self, address):
		return requests.post('http://'+ address + '/kv-store/view-change/receive', data= json.dumps(self.totalMsgVector), timeout=20)

	# followers
	# just send message 
	def prime(self, host, newView):

		self.changingView = True
		self.stagedMessages = {}

		self.view = newView.split(',')
		print(len(self.view))
		#determine message vector
		messageVector = (0, )* len(self.view)
		for key, value in self.dictionary:
			 for i in range(0, len(newView)):
			 	destination = determineDestination(key, newView)
			 	if newView[i] == destination and destination != host:
			 		messageVector[i] = messageVector[i] + 1
			 		stagedMessages[key] = destination
		print("prime...")
		print(messageVector)
		# return jsonify(messageVector)
		# return Response(jsonify({"messageVector": messageVector}), status=200,mimetype='application/json')
		return jsonify({"messageVector": messageVector}), 200

		# return jsonify(messageVector)
		#send primeAck w/ vector
		#stage those messages



	def startChange(self, msgVector):
		req_data = request.get_json(silent=True)
		# messageVector = req_data["messageVector"]

		# store message vector
		self.totalMsgVector = msgVector

		# send staged messages
		# for key, address in self.stagedMessages:
		# 	sendKeyValue(key, address)
		if len(self.stagedMessages) > 0:
			messagePool = ThreadPool(len(self.stagedMessages))
			messagePool.map(sendKeyValue, self.stagedMessages)
			messagePool.close()

			messagePool.join()

		receivedFinalMessage = True
		for entry in self.totalMsgVector:
			if entry != 0:
				receivedFinalMessage = False
		self.finalMsgReceive = receivedFinalMessage

		if self.finalMsgReceive == False:
			self.receiveFinalMessageEvent.wait()

		for key, address in self.stagedMessages:
			self.dictionary.remove(key)

		return {"address": request.host, "key-count": len(self.dictionary)}



	def receiveValue(self, key, value, sender):
		# store that dang thing and decrement the correct element of the message vector
		# if all elements of the vector are zero, send done message with my final message total 
		# set change view to false here only if not arbiter

		self.dictionary[key] = value
		for i in range(0, len(self.view)):
			if self.view[i] == sender:
				self.totalMsgVector[i] = self.totalMsgVector[i] - 1

		receivedFinalMessage = True
		for entry in self.totalMsgVector:
			if entry != 0:
				receivedFinalMessage = False

		if receivedFinalMessage:
			self.receiveFinalMessageEvent.set()

	def sendKeyValue(self, key, address):
		return requests.put('http://'+ address + '/kv-store/view-change/receive?key=' + key + '&value=' + self.dictionary[key],timeout=20)

	# return jsonify of dict size 
	def getKeyCount(self):
		return jsonify({"message": "Key count retrieved successfully", "key-count": len(self.dictionary)}), 200 

	# returns ip address 
	def determineDestination(value):
		return