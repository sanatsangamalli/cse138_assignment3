from flask import Flask, request, render_template, jsonify
import os
import sys
import hashlib
import requests
import json

class mainKeyVal:

	def __init__(self):
		self.dictionary = {}
		self.ip_list = []

	def hash_key(self, key_value):
		hexVal = int(hashlib.sha1(key_value.encode('utf-8')).hexdigest(), 16)
		return self.ip_list[hexVal % len(self.ip_list)]

	def get(self, request, key_name):
		if key_name in self.dictionary:
			return jsonify({"doesExist":True, "message":"Retrieved successfully", "value":self.dictionary[key_name]}), 200
		else:
			if len(self.ip_list) != 0: # Make sure list is non empty
				key_hash = self.hash_key(key_name)
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
			if len(self.ip_list) != 0: # Make sure list is non empty
				key_hash = self.hash_key(key_name)
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
			
	def delete(self, request, key_name):
		if key_name in self.dictionary:
			del self.dictionary[key_name]
			return jsonify({"doesExist":True, "message":"Deleted successfully"}), 200
		else:
			if len(self.ip_list) != 0: # Make sure list is non empty
				key_hash = self.hash_key(key_name)
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