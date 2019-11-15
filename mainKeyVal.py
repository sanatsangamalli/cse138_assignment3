from flask import Flask, request, render_template, jsonify, Response
import os
import sys
import hashlib
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
		self.view.sort()

		self.leadingViewChange = False
		self.expectedReceiveCount = 0
		self.receiveFinalMessageEvent = threading.Event()

	def determineDestination(self, key_value):
		hexVal = int(hashlib.sha1(key_value.encode('utf-8')).hexdigest(), 16)
		return self.view[hexVal % len(self.view)]

	def get(self, request, key_name):
		if key_name in self.dictionary:
			return jsonify({"doesExist":True, "message":"Retrieved successfully", "value":self.dictionary[key_name]}), 200
		else:
			if len(self.view) != 0: # Make sure list is non empty
				key_hash = self.determineDestination(key_name)
				if os.environ['ADDRESS'] == key_hash:
					return jsonify({"doesExist":False, "error:":"Key does not exist", "message":"Error in GET"}), 404
				else:
					print("Forwarding request to ", key_hash)
					try:
						req_data = request.get_json(silent=True)
						if req_data is not None:
							response = requests.get('http://'+ key_hash + '/kv-store/keys/' + key_name, data=json.dumps(req_data), headers=dict(request.headers), timeout=20)
						else:
							response = requests.get('http://'+ key_hash + '/kv-store/keys/' + key_name, headers=dict(request.headers), timeout=20)
					except:
						return jsonify({'error': 'Node in view (' + key_hash + ') does not exist', 'message': 'Error in GET'}), 503
					json_response = response.json()
					json_response.update({'address': key_hash})
					return json_response, response.status_code
					#return response.content, response.status_code
			return jsonify({'error': 'Missing VIEW environmental variable', 'message': 'Error in GET'}), 503
			
					
	def put(self, request, key_name):
		if len(key_name) > 50:
			return jsonify({"error:":"Key is too long", "message":"Error in PUT"}), 400
		print(len(key_name))
		req_data = request.get_json(silent=True)
		if req_data is not None and 'value' in req_data:
			if len(self.view) != 0: # Make sure list is non empty
				key_hash = self.determineDestination(key_name)
				if os.environ['ADDRESS'] == key_hash:
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
					print("Forwarding request to ", key_hash)
					try:
						response = requests.put('http://'+ key_hash + '/kv-store/keys/' + key_name, data=json.dumps(req_data), headers=dict(request.headers), timeout=20)
					except:
						return jsonify({'error': 'Node in view (' + key_hash + ') does not exist', 'message': 'Error in PUT'}), 503
					json_response = response.json()
					json_response.update({'address': key_hash})
					return json_response, response.status_code
			else:
				return jsonify({"error:":"Value is missing", "message":"Error in PUT"}), 400
		else:
			return jsonify({"error:":"Value is missing", "message":"Error in PUT"}), 400
			
	def delete(self, request, key_name):
		if key_name in self.dictionary:
			del self.dictionary[key_name]
			return jsonify({"doesExist":True, "message":"Deleted successfully"}), 200
		else:
			if len(self.view) != 0: # Make sure list is non empty
				key_hash = self.determineDestination(key_name)
				if os.environ['ADDRESS'] == key_hash:
					return jsonify({"doesExist":False, "error:":"Key does not exist", "message":"Error in DELETE"}), 404    
				else:
					print("Forwarding request to ", key_hash)
					try:
						req_data = request.get_json(silent=True)
						if req_data is not None:
							response = requests.delete('http://'+ key_hash + '/kv-store/keys/' + key_name, data=json.dumps(req_data), headers=dict(request.headers), timeout=20)
						else:
							response = requests.delete('http://'+ key_hash + '/kv-store/keys/' + key_name, headers=dict(request.headers), timeout=20)
					except:
						return jsonify({'error': 'Node in view (' + key_hash + ') does not exist', 'message': 'Error in DELETE'}), 503
					json_response = response.json()
					json_response.update({'address': key_hash})
					return json_response, response.status_code
					#return response.content, response.status_code
			return jsonify({'error': 'Missing VIEW environmental variable', 'message': 'Error in GET'}), 503
	

	# arbiter 
	# request.get_json 
	def viewChange(self, request):
		# send prime message with view to other ip address in view 
		self.changingView = True
		self.leadingViewChange = True
		req_data = request.get_json(silent=True)
		newView = req_data["view"]
		receivers = newView.split(',')
		print('Receivers = ', receivers)
		request.host = os.environ['ADDRESS'] # REMOVE LATER (maybe)
		print('Request.host = ', request.host)
		receivers.remove(request.host)

		self.view = newView.split(',')
		self.view.sort()

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
		print("totalMsgVector")
		print(self.totalMsgVector)


		self.receiveFinalMessageEvent = threading.Event() 
		# if we've received the last one, send start message with final total
		# each element should be of format i.e. { "address": "10.10.0.2:13800", "key-count": 5 },
		print("Hostshard waiting for ", self.totalMsgVector[request.host])
		hostShard = self.startChange(self.totalMsgVector[request.host]).get_json()
		
		shardPool = ThreadPool(len(receivers))
		shards = shardPool.map(self.sendStartMessage, receivers)
		shardPool.close()

		if self.expectedReceiveCount > 0:
			print('waiting for receiveFinalMessageEvent')
			self.receiveFinalMessageEvent.wait()

		shardPool.join()


		# finalShards
		for shard in shards:
			shards[shards.index(shard)] = {"address": shard.json()["address"], "key-count": shard.json()["key-count"]}
		hostShard['key-count'] = len(self.dictionary)
		shards.append(hostShard)
		self.changingView = False
		self.leadingViewChange = False
		return jsonify({"message": "View change successful", "shards": shards}), 200 



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
		self.view.sort()
		self.expectedReceiveCount = 0
		#determine message vector
		#messageVector = (0, )* len(self.view)
		messageVector = {}
		for address in self.view:
				messageVector[address] = 0

		for key in self.dictionary:
			destination = self.determineDestination(key)
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



	def startChange(self, receiveCount):
		# store message vector
		print("STARTING STARTCHANGE: ", receiveCount)
		self.expectedReceiveCount += receiveCount

		# send staged messages
		# for key, address in self.stagedMessages:
		#   sendKeyValue(key, address)
		if len(self.stagedMessages) > 0:
			messagePool = ThreadPool(len(self.stagedMessages))
			messagePool.map(self.sendKeyValue, self.stagedMessages)
			messagePool.close()

			messagePool.join()

		
		#while self.expectedReceiveCount > 0:
		#	None

		if self.leadingViewChange == False and self.expectedReceiveCount > 0:
			print('waiting for receiveFinalMessageEvent')
			self.receiveFinalMessageEvent.wait()

		print("Test stagedMessages")
		for key in self.stagedMessages:
			del self.dictionary[key]

		self.changingView = False
		
		print("DICT AT END OF STARTCHANGE: ", self.dictionary)
		print(jsonify({"address": request.host, "key-count": len(self.dictionary)}).json)
		return jsonify({"address": request.host, "key-count": len(self.dictionary)})



	def receiveValue(self, key, value, sender):
		# store that dang thing and decrement the correct element of the message vector
		# if all elements of the vector are zero, send done message with my final message total 
		# set change view to false here only if not arbiter

		self.dictionary[key] = value

		self.expectedReceiveCount -= 1

		print("receiving value ... left to receive: " + str(self.expectedReceiveCount))

		if self.expectedReceiveCount == 0:
			self.receiveFinalMessageEvent.set()
		return jsonify({"message":"Success"}), 200

	def sendKeyValue(self, key):
		print("Dest: "+ self.stagedMessages[key] + ", Key = " + key)
		return requests.put('http://'+ self.stagedMessages[key] + '/kv-store/view-change/receive?key=' + key + '&value=' + self.dictionary[key],timeout=20)

	# return jsonify of dict size 
	def getKeyCount(self):
		return jsonify({"message": "Key count retrieved successfully", "key-count": len(self.dictionary)}), 200 
