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

		self.totalMsgVector = self.prime(request.host, newView)[0].get_json()

		pool.join()

		# add message vector to our running total
		for response in resultingMsgVectors:
			vectorResponse = response.json()
			for address in self.totalMsgVector:
				self.totalMsgVector[address] += vectorResponse[address]

		self.receiveFinalMsg = threading.Event() 
		# if we've received the last one, send start message with final total
		# each element should be of format i.e. { "address": "10.10.0.2:13800", "key-count": 5 },
		shardPool = ThreadPool(len(receivers))
		shards = shardPool.map(self.sendStartMessage, receivers)
		shardPool.close()

		hostShard = self.startChange(self.totalMsgVector[request.host]).get_json()

		shardPool.join()

		print('shard')
		# finalShards
		for shard in shards:
			print(type(shard))
			shards[shards.index(shard)] = {"address": shard.json()["address"], "key-count": shard.json()["key-count"]}
			print(shard)
		print(hostShard)
		print(type(hostShard))
		shards.append(hostShard)
		print('shards')
		print(type(shards))
		print(shards)

		return jsonify({"message": "View change successful", "shards": shards}), 200 
		# self.completedViewChange = threading.Event()
		# self.completedViewChange.wait()



	def sendPrimeMessage(self, address):
		return requests.get('http://'+ address + '/kv-store/view-change/receive?view='+ (",".join(self.view)),timeout=20)

	def sendStartMessage(self, address):
		print('sendStartMessage totalMsgVector')
		print(self.totalMsgVector)
		return requests.post('http://'+ address + '/kv-store/view-change/receive?count=' + str(self.totalMsgVector[address]), timeout=20)

	# followers
	# just send message 
	def prime(self, host, newView):

		self.changingView = True
		self.stagedMessages = {}

		self.view = newView.split(',')
		#determine message vector
		#messageVector = (0, )* len(self.view)
		messageVector = {}
		for address in self.view:
				messageVector[address] = 0

		for key, value in self.dictionary:
			destination = determineDestination(key)
			if destination != host:
				messageVector[destination] += 1
				self.stagedMessages[key] = destination

		"""for key, value in self.dictionary:
			 for i in range(0, len(newView)):
			 	destination = determineDestination(key, newView)
			 	if newView[i] == destination and destination != host:
			 		messageVector[i] = messageVector[i] + 1
			 		stagedMessages[key] = destination
 		"""
		print("prime...")
		print(messageVector)
		# return jsonify(messageVector)
		# return Response(jsonify({"messageVector": messageVector}), status=200,mimetype='application/json')
		return jsonify(messageVector), 200

		# return jsonify(messageVector)
		#send primeAck w/ vector
		#stage those messages



	def startChange(self, expectedReceiveCount):
		# store message vector
		self.expectedReceiveCount = expectedReceiveCount

		# send staged messages
		# for key, address in self.stagedMessages:
		# 	sendKeyValue(key, address)
		if len(self.stagedMessages) > 0:
			messagePool = ThreadPool(len(self.stagedMessages))
			messagePool.map(sendKeyValue, self.stagedMessages)
			messagePool.close()

			messagePool.join()

		if self.expectedReceiveCount > 0:
			self.receiveFinalMessageEvent.wait()

		for key, address in self.stagedMessages:
			self.dictionary.remove(key)

		self.changingView = False

		return jsonify({"address": request.host, "key-count": len(self.dictionary)})



	def receiveValue(self, key, value, sender):
		# store that dang thing and decrement the correct element of the message vector
		# if all elements of the vector are zero, send done message with my final message total 
		# set change view to false here only if not arbiter

		self.dictionary[key] = value

		self.expectedReceiveCount -= 1

		if self.expectedReceiveCount == 0:
			self.receiveFinalMessageEvent.set()

	def sendKeyValue(self, key, address):
		return requests.put('http://'+ address + '/kv-store/view-change/receive?key=' + key + '&value=' + self.dictionary[key],timeout=20)

	# return jsonify of dict size 
	def getKeyCount(self):
		return jsonify({"message": "Key count retrieved successfully", "key-count": len(self.dictionary)}), 200 

	# returns ip address 
	def determineDestination(value):
		return