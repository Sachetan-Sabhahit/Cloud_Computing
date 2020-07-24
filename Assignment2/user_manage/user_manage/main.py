from flask import Flask, render_template, jsonify, request,abort
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey
import requests
import json
from random import randint
import csv
from datetime import datetime as dt
import re


server = "localhost"
#server = '10.20.200.163'
user_port = '8080'
ride_port = '8000'

user_db_write_url = "http://" + server + ":" + user_port + "/api/v1/db/write"
user_db_read_url = "http://" + server + ":" + user_port + "/api/v1/db/read"

ride_db_write_url = "http://" + server + ":" + ride_port + "/api/v1/db/write"
ride_db_read_url = "http://" + server + ":" + ride_port + "/api/v1/db/read"

engine = sql.create_engine('sqlite:///database/RideShare.db', echo=True)

meta = sql.MetaData()
user_details = Table('UserDetails', meta, 
	Column('username', String, primary_key=True),
	Column('password', String),
	)

'''ride_details = Table('RideDetails', meta, 
	Column('ride_id', Integer, primary_key=True),
	Column('created_by', String),
	Column('source', String),
	Column('destination', String),
	Column('timestamp', String),
	Column('riders_list', String),
	)'''

meta.create_all(engine)

# To check if the user exists
def userExists(name):
	# engine = sql.create_engine('sqlite:///RideShare.db', echo=True)
	conn = engine.connect()
	res = conn.execute("SELECT * FROM UserDetails WHERE username=$name", name)
	res = list(res)
	if len(res) > 0:
		return True
	return False

# To check if the password is valid
def rightPassword(password):
	possibleChar = set([str(i) for i in range(10)]).union({'a', 'b', 'c', 'd', 'e', 'f'})
	password = password.lower()
	print(set(password).difference(possibleChar))
	if len(password) == 5 and len(set(password).difference(possibleChar)) == 0:
		return True
	return False

# To check if time is not valid
def wrongTime(time):
	if re.search("^\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d$", time):
		return False
	return True

# To add a new user
def addUser(username, password):
	r = requests.post(user_db_write_url, json={"table":"UserDetails", "column":["username", "password"],
										'insert':[username, password], "action":"insert", "where":""})
	# print("HERE:", r.status_code)


# To remove a user
def removeUser(username):
	conn = engine.connect()
	r = requests.post(user_db_write_url, json={"table":"UserDetails", "column":[], "insert":[], "action":"delete",
										"where":"username='" + username + "'"})

	res = conn.execute("SELECT * FROM UserDetails")
	print(list(res))
		

# To check if the user is part of the ride
def userInRide(username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["riders_list"], "where":""})
	r = r.json()
	print("IN USER_IN_RIDE(user):")
	print(r)
	for i in r:
		i = i[0].split(",")[:-1]
		if username in i:
			return True
	return False

# To get the list of all users
def getAllUsers():
	#user_db_read_url = "http://0.0.0.0/api/v1/db/read"
	r = requests.post(user_db_read_url, json={"table":"UserDetails", "columns":["username"], "where":""})
	r = r.json()
	l = []
	for i in r:
		l.append(i[0])
	return l
	 
app=Flask(__name__)
# 200 - OK
# 201 - Created
# 204 - No Content 
# 400 - Bad Request
# 405 - Method Not Allowed
# 500 - Internal Server Error



@app.route("/")
def greet():
	return "Here at last"


# 1
@app.route("/api/v1/users", methods=["PUT", "GET"])
def add_user():
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
		print(usersList)
		return usersList
		
		
# [[23], [162], [236], [247], [346], [472], [659], [695], [739], [768], [832]]
	
# 2
@app.route("/api/v1/users/<username>", methods=["DELETE"])
def remove_user(username):
	if userExists(username):
		if not userInRide(username):
			removeUser(username)
			return jsonify({}), 200
		else:
			abort(400, "User is part of a ride. Please end the ride first")
	else:
		abort(400, "User does not exist")
		
		
# 3
@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
	conn = engine.connect()
	conn.execute("DROP TABLE UserDetails")
	meta = sql.MetaData()
	user_details = Table('UserDetails', meta, 
		Column('username', String, primary_key=True),
		Column('password', String),
	)
	meta.create_all(engine)
	return jsonify({})
	

# 8
@app.route('/api/v1/db/write', methods=["POST"])
def write_db():
	print("in db write")
	queryData = request.get_json()
	try:
		values = queryData['insert'] 
		columns = queryData['column'] 
		table = queryData['table']
		action = queryData['action']
		condition = queryData['where']

		if action == "insert":
			conn = engine.connect()
			query = "INSERT INTO " + table + "("
			for i in columns:
				query += i + ","
			query = query[:-1] + ") VALUES("
			for i in values:
				if type(i) == str:
					query += "'" + i + "',"
				elif type(i) == int:
					query += str(i) + ","
				else:
					print("UNSUPPORTED DATA-TYPE")
					exit(0)
			query = query[:-1] + ")"
			print(".\n"*5 + query)
			conn.execute(query)
			res = conn.execute("SELECT * FROM " + table)
			print(list(res))
			print("HHHHHHHHHHHHHHHHere")

		elif action == "update":
			conn = engine.connect()
			query = "UPDATE " + table + " SET " + columns[0] + "='" + values + "' WHERE " + condition
			print(".\n"*5 + query)
			conn.execute(query)

			res = conn.execute("SELECT * FROM " + table)
			print(list(res))
			
		elif action == "delete":
			conn = engine.connect()
			query = "DELETE FROM "+ table + " WHERE " + condition
			print(query)
			conn.execute(query)

			res = conn.execute("SELECT * FROM " + table)
			print(list(res))
			return jsonify({}), 200
		return "OK"
	except KeyError:
		abort(400, "Please provide proper JSON request body")

# 9
@app.route('/api/v1/db/read', methods=["POST"])
def read_db():
	print("\tHERE")
	queryData = request.get_json()
	print("\tAND HERE")
	print(queryData)
	try:
		table = queryData['table']
		columns = queryData['columns']
		condition = queryData['where']
		print(".\n"*10)
		conn = engine.connect()
		# res = conn.execute("SELECT * FROM RideDetails")
		# res = conn.execute("SELECT ride_id,created_by FROM RideDetails")
		query = "SELECT " + ",".join(columns) + " FROM " + table
		if condition:
			query += " WHERE " + condition
		print(".\n" * 5, query)
		res = conn.execute(query)
		res = list(res)
		for index, _ in enumerate(res):
			res[index] = tuple(res[index])
		print(res)
		return json.dumps(res)
	except KeyError:
		abort(400, "Please provide proper JSON request body")


		
if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0", port=8080)
