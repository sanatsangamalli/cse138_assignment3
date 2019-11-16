from flask import Flask, request, render_template, jsonify, Response
from multiprocessing.dummy import Pool as ThreadPool
import os
import sys
import hashlib
import threading
import requests
import json
import operator


class mainKeyVal:

	def __init__(self, myView):
		self.dictionary = {} # Dictionary containing key-value pairs for this node
		self.view = myView.split(',')
		self.view.sort() # Sort the view to ensure nodes can reference each other consistently
		self.leadingViewChange = False # Is the current node the "leader" (initiating the view change)
		self.changingView = False # Is view currently being changed
		self.expectedReceiveCount = 0 # Number of keys this node is expected to have after a new view change
		self.receiveFinalMessageEvent = threading.Event() # Threading event to determine when node needs to wait

	# Hash partitioning
	# Given a key, function returns the IP address of the node that key will be stored on
	def determineDestination(self, key_value):
		hashVal = int(hashlib.sha1(key_value.encode('utf-8')).hexdigest(), 16) # First hash is returned as hex value, then converted
		return self.view[hashVal % len(self.view)]

	# Fulfills client GET requests
	def get(self, request, key_name):
		if key_name in self.dictionary:
			return jsonify({"doesExist":True, "message":"Retrieved successfully", "value":self.dictionary[key_name]}), 200
		else:
			if len(self.view) != 0: # Make sure list is non empty
				key_hash = self.determineDestination(key_name) # Hash the key

				if os.environ['ADDRESS'] == key_hash:
					return jsonify({"doesExist":False, "error:":"Key does not exist", "message":"Error in GET"}), 404
				else:
					# If key belongs to another node, forward request
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
			return jsonify({'error': 'Missing VIEW environmental variable', 'message': 'Error in GET'}), 503
			
	# Fulfills client PUT requests
	def put(self, request, key_name):
		if len(key_name) > 50:
			return jsonify({"error:":"Key is too long", "message":"Error in PUT"}), 400
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
					try:
						response = requests.put('http://'+ key_hash + '/kv-store/keys/' + key_name, data=json.dumps(req_data), headers=dict(request.headers), timeout=20)
					except:
						return jsonify({'error': 'Node in view (' + key_hash + ') does not exist', 'message': 'Error in PUT'}), 503
					json_response = response.json()
					json_response.update({'address': key_hash})
					return json_response, response.status_code
			else:
				return jsonify({"error:":"List is empty", "message":"Error in PUT"}), 400
		else:
			return jsonify({"error:":"Value is missing", "message":"Error in PUT"}), 400
	
	# Fulfills client DELETE requests
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
			return jsonify({'error': 'Missing VIEW environmental variable', 'message': 'Error in GET'}), 503
	

	# Initiate a view change
	# Function is called by the node that received the view change request 
	def viewChange(self, request):
		# send prime message with view to other ip address in view 
		self.changingView = True
		self.leadingViewChange = True
		req_data = request.get_json(silent=True)
		newView = req_data["view"] # Variable containing the new view
		receivers = newView.split(',')
		request.host = os.environ['ADDRESS']
		receivers.remove(request.host)

		self.view = newView.split(',')
		self.view.sort()

		# Create multiple threads to send a prime message to all other nodes in the view
		pool = ThreadPool(len(receivers))
		resultingMsgVectors = pool.map(self.sendPrimeMessage, receivers)
		pool.close()

		self.totalMsgVector = self.prime(request.host, newView)[0].get_json()

		pool.join()

		
		# Add message vector to our running total, to keep track of how many keys each node will have
		for response in resultingMsgVectors:
			vectorResponse = response.json()
			for address in self.totalMsgVector:
				self.totalMsgVector[address] += vectorResponse[address]

		self.receiveFinalMessageEvent = threading.Event() 
		# If we've received the last one, send start message with final total
		# each element should be of format i.e. { "address": "10.10.0.2:13800", "key-count": 5 },

		hostShard = self.startChange(self.totalMsgVector[request.host]).get_json() # Leading node send out its keys first
		shardPool = ThreadPool(len(receivers))
		shards = shardPool.map(self.sendStartMessage, receivers) # Signal other nodes to send keys
		shardPool.close()

		# Leading node waits to receive all its keys
		if self.expectedReceiveCount > 0:
			self.receiveFinalMessageEvent.wait()

		shardPool.join()

		# Loop through shards to get all key counts and construct JSON response
		for idx, shard in enumerate(shards):
			shards[idx] = {"address": shard.json()["address"], "key-count": shard.json()["key-count"]}
		hostShard['key-count'] = len(self.dictionary)
		shards.append(hostShard) # Append the leading node's shard
		self.changingView = False
		self.leadingViewChange = False

		return jsonify({"message": "View change successful", "shards": shards}), 200 

	# Leading node send prime message to all nodes in view
	def sendPrimeMessage(self, address):
		return requests.get('http://'+ address + '/kv-store/view-change/receive?view='+ (",".join(self.view)),timeout=20)

	# Leading node send start message to all nodes in view
	def sendStartMessage(self, address):
		return requests.post('http://'+ address + '/kv-store/view-change/receive?count=' + str(self.totalMsgVector[address]), timeout=20)

	# followers
	# Prepare for a view change by determining how many keys will be sent to each node
	def prime(self, host, newView):

		self.changingView = True
		self.stagedMessages = {}

		self.view = newView.split(',')
		self.view.sort() # Determine the new view for a follower
		self.expectedReceiveCount = 0 # Number of keys a node is expected to receive
		# Initialize message vector
		messageVector = {}
		for address in self.view:
			messageVector[address] = 0

		# Determine how many keys need to be sent to each node
		for key in self.dictionary:
			destination = self.determineDestination(key)
			if destination != host:
				messageVector[destination] += 1
				self.stagedMessages[key] = destination

		return jsonify(messageVector), 200

	# Send out all keys that do not belong to the current node
	def startChange(self, receiveCount):
		# Store message vector
		self.expectedReceiveCount += receiveCount

		# Send staged messages, if any
		if len(self.stagedMessages) > 0:
			messagePool = ThreadPool(len(self.stagedMessages))
			messagePool.map(self.sendKeyValue, self.stagedMessages)
			messagePool.close()
			messagePool.join()

		# Block if not the leading node and this node is still expecting a key (to be sent from another node)
		if self.leadingViewChange == False and self.expectedReceiveCount > 0:
			self.receiveFinalMessageEvent.wait()

		# Delete all keys that have been sent
		for key in self.stagedMessages:
			del self.dictionary[key]

		self.changingView = False
		return jsonify({"address": request.host, "key-count": len(self.dictionary)})


	# store that dang thing and decrement the correct element of the message vector
	# if all elements of the vector are zero, send done message with my final message total 
	# set change view to false here only if not arbiter
	def receiveValue(self, key, value, sender):

		self.dictionary[key] = value
		self.expectedReceiveCount -= 1

		if self.expectedReceiveCount == 0:
			self.receiveFinalMessageEvent.set() # Signal threading event to stop blocking (all expected keys received)
		return jsonify({"message":"Success"}), 200

	# Send a key to another node
	def sendKeyValue(self, key):
		return requests.put('http://'+ self.stagedMessages[key] + '/kv-store/view-change/receive?key=' + key + '&value=' + self.dictionary[key],timeout=20)

	# return jsonify of dict size 
	def getKeyCount(self):
		return jsonify({"message": "Key count retrieved successfully", "key-count": len(self.dictionary)}), 200 
