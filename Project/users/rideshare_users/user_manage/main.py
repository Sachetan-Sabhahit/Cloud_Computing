from flask import Flask, render_template, jsonify, request,abort
import requests
import json
from random import randint
import csv
from datetime import datetime as dt
import re
import logging

user_server = "18.235.245.250"
ride_server = "18.235.245.250"

user_port = '80'
ride_port = '80'

user_db_write_url = "http://" + user_server + ":" + user_port + "/api/v1/db/write"
user_db_read_url = "http://" + user_server + ":" + user_port + "/api/v1/db/read"

ride_db_write_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/write"
ride_db_read_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/read"

HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH', 'COPY']

#to take care of the count in orchestrator when 405 or 204 requests are sent 
def dummyRequest():
	r = requests.post(user_db_read_url, json={"table":"UserDetails", "columns":["*"], "where":""})

def userExists(name):
	r = requests.post(ride_db_read_url, json={"table":"UserDetails", "columns":["*"], "where":"username='"+name+"'"})
	res = r.json()
	if len(res) > 0:
		return True
	return False

def rightPassword(password):
	possibleChar = set([str(i) for i in range(10)]).union({'a', 'b', 'c', 'd', 'e', 'f'})
	password = password.lower()
	if len(password) == 40 and len(set(password).difference(possibleChar)) == 0:
		return True
	return False


def wrongTime(time):
	if re.search("^\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d$", time):
		return False
	return True


def addUser(username, password):
	r = requests.post(user_db_write_url, json={"table":"UserDetails", "columns":["username", "password"],
										'insert':[username, password], "action":"insert", "where":""})


def removeUser(username):
	r = requests.post(user_db_write_url, json={"table":"UserDetails", "columns":[], "insert":[], "action":"delete",
										"where":"username='" + username + "'"})

def userInRide(username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["riders_list"], "where":""})
	r = r.json()
	for i in r:
		i = i[0].split(",")[:-1]
		if username in i:
			return True
	return False

def getAllUsers():
	r = requests.post(user_db_read_url, json={"table":"UserDetails", "columns":["username"], "where":""})
	r = r.json()
	l = []
	for i in r:
		l.append(i[0])
	return l


def addCount():
	f = open("count.txt", 'r')
	d = f.readlines()[0]
	f.close()
	count = int(d)
	f = open("count.txt", 'w')
	f.write(str(count+1))
	f.close()

app=Flask(__name__)
f = open("count.txt", 'a')
f.write('0')
f.close()
# 200 - OK
# 201 - Created
# 204 - No Content 
# 400 - Bad Request
# 405 - Method Not Allowed
# 500 - Internal Server Error



@app.route("/")
def greet():
	return "Here at last in Users"


# 1
@app.route("/api/v1/users", methods=HTTP_METHODS)
def add_user():
	addCount()
	print(request.method)
	if request.method == "PUT":
		bad_request = False
		userinfo = request.get_json()
		try:
			username = userinfo["username"]
			password = userinfo["password"]
			if not userExists(username):
				if rightPassword(password):
					addUser(username, password)
					return jsonify({}), 201
				else:
					abort(400, "Password format violated")
			else:							# Invalid username / password
				abort(400, "Username already exists")
		except KeyError:					# Invalid JSON input
			abort(400, "Provide proper JSON request body")

	elif request.method == "GET":
		usersList = jsonify(getAllUsers())
		# print(usersList)
		return usersList
	else:
		dummyRequest()
		abort(405, "I don't accept this method")

# 2
@app.route("/api/v1/users/<username>", methods=HTTP_METHODS)
def remove_user(username):
	addCount()
	if request.method == "DELETE":
		if userExists(username):
			if not userInRide(username):
				removeUser(username)
				return jsonify({}), 200
			else:
				abort(400, "User is part of a ride. Please end the ride first")
		else:
			abort(400, "User does not exist")
	else:
		dummyRequest()
		abort(405, "I don't accept this method")



@app.route('/api/v1/_count', methods=["GET", "DELETE"])
def countCalls():
	if request.method == "GET":
		l =[]
		f = open("count.txt", 'r')
		d = f.readlines()[0]
		f.close()
		l.append(int(d))
		return jsonify(l)
	elif request.method == "DELETE":
		f = open("count.txt", 'w')
		f.write('0')
		f.close()
		return jsonify([]), 200


if __name__ == '__main__':
	app.debug=True
	app.run(host="0.0.0.0")

