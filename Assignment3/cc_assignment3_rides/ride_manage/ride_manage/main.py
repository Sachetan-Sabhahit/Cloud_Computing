from flask import Flask, render_template, jsonify, request,abort
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey
import requests
import json
from random import randint
import csv
from datetime import datetime as dt
import re
import logging


# API_COUNT = 0
#RIDE_ID = 1
HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

#server = '10.20.200.163'
user_server = "18.206.80.77"
#user_server = '10.20.200.163'
ride_server = "35.168.94.255"

load_balancer = "A3-1156341838.us-east-1.elb.amazonaws.com"

user_port = '80'
ride_port = '80'

user_db_write_url = "http://" + user_server + ":" + user_port + "/api/v1/db/write"
user_db_read_url = "http://" + user_server + ":" + user_port + "/api/v1/db/read"

ride_db_write_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/write"
ride_db_read_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/read"

engine = sql.create_engine('sqlite:///database/RideShare.db', echo=True)


# songs = relationship("Song", cascade="all, delete", passive_deletes=True)
meta = sql.MetaData()
ride_details = Table('RideDetails', meta, 
	Column('ride_id', Integer, primary_key=True),
	Column('created_by', String),
	Column('source', String),
	Column('destination', String),
	Column('timestamp', String),
	Column('riders_list', String),
	)

meta.create_all(engine)

# To check if the user exists
def userExists(name):
	#r = requests.post(user_db_read_url, json={"table":"UserDetails", "columns":["username"], "where":"username='" + name + "'"})
	# print("."*10, r)
	r = requests.get("http://"+load_balancer+"/api/v1/users", headers={"Origin":"35.168.94.255"})
	r = r.json()
	# print("."*10, r)
	if name in r:
		return True
	return False

# To check if time is not valid
def wrongTime(time):
	if re.search("^\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d$", time):
		return False
	return True

# To add a ride
def addRide(created_by, timestamp, source, destination):
	# global RIDE_ID
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["ride_id"],
										'where':""})
	# print(r.json())
	ride_ids = r.json()
	while True:
		new_ride_id = randint(0, 100000000)
		if new_ride_id not in ride_ids:
			break
	
	# conn = engine.connect()
	# res = conn.execute("SELECT * FROM RideDetails")
	# print(list(res))
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "column":["ride_id", "created_by", "timestamp",
											"source", "destination", "riders_list"], 
											'insert':[new_ride_id, created_by, timestamp, source, destination,
														created_by+','], "action":"insert", "where":""})

	# print("HERE:", r.status_code)
	# res = conn.execute("SELECT * FROM RideDetails")
	# print("Finally:", list(res))
	# RIDE_ID += 1

# To check if the area pair is recorded
def areaPairRecorded(source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["source", "destination"],
											 "where":""})
	for i in r.json():
		if i[0] == source and i[1] == destination:
			return True
	return False


# To get all the rides
def getRides(source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
						"columns":["ride_id", "created_by", "timestamp"],
						"where":"source="+str(source)+" AND destination="+str(destination)})
	r = r.json()
	# print("8==D\n"*10, r)
	d = []
	for i  in r:
		dt_object1 = dt.strptime(i[2], "%d-%m-%Y:%S-%M-%H")
		if dt_object1 > dt.now():
			d.append({"rideId":i[0], "username": i[1] , "timestamp":i[2]})
	# print("8===D\n"*10, d)
	return d


# To check if a ride exists
def rideExists(rideId):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["ride_id"], 
											"where":"ride_id="+rideId})
	r = r.json()
	if len(r) > 0:
		return True
	return False

# To get the details of the ride
def rideDetails(ride_id):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
											"columns":["ride_id", "created_by", "riders_list", "timestamp",
														"source", "destination"],
											"where":"ride_id=" + ride_id})
	r = r.json()
	d = {"rideId":r[0][0], "Created_by":r[0][1], "users":r[0][2].split(',')[:-1],
			"Timestamp":r[0][3], "source":r[0][4], "destination":r[0][5]}
	return d

# To join the ride
def joinRide(ride_id, username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
											"columns":["riders_list"],"where":"ride_id=" + ride_id})
	riders_list = r.json()[0][0]
	updated_riders_list = riders_list + username + ","
	# print("UPDATED:", updated_riders_list)

	r = requests.post(ride_db_write_url, json={"table":"RideDetails",
										"column":["riders_list"], "action":"update",
										 "where":"ride_id="+ str(ride_id), "insert":updated_riders_list})
	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	# print(list(res))

# To remove a ride
def removeRide(ride_id):
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "column":[], "action":"delete",
											"where":"ride_id='" + ride_id + "'", "insert":""})

	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	# print(list(res))
	
# To check if it is a valid area
def validAreas(l):
	# print("*\n"*10, "YO")
	f = open("AreaNameEnum.csv", 'r')
	data = list(csv.reader(f))
	f.close()
	number, area = zip(*data)
	for i in l:
		if str(i) not in number:
			return False
	return True

# To check if the user is part of the ride
def userInRide(username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["riders_list"], "where":""})
	r = r.json()
	# print("IN USER_IN_RIDE:")
	# print(r)
	for i in r:
		i = i[0].split(",")[:-1]
		if username in i:
			return True
	return False

# To get the rides count
def getRidesCount():
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["COUNT(*)"], "where":""})
	res = r.json()
	return res[0]

# To increment the rides count
def addCount():
	f = open("count.txt", 'r')
	d = f.readlines()[0]
	f.close()
	count = int(d)
	f = open("count.txt", 'w')
	f.write(str(count+1))
	f.close()



app=Flask(__name__)
f = open('count.txt', 'w')
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
	# addCount()
	return "Here at last in Rides"


# 3
@app.route("/api/v1/rides", methods=HTTP_METHODS)
def create_new_ride():
	print("8=D\n"*10, request.method)
	addCount()
	if request.method == "POST":
		rideinfo = request.get_json()
		try:
			created_by = rideinfo["created_by"]
			timestamp = rideinfo["timestamp"]
			source = int(rideinfo["source"])
			destination = int(rideinfo["destination"])
			if not validAreas([source, destination]):
				abort(400, "Invalid areas")
			elif not userExists(created_by):
				abort(400, "User does not exist")
			elif wrongTime(timestamp):
				abort(400, "Invalid timestamp")
			else:
				addRide(created_by, timestamp, source, destination)
		except KeyError:
			abort(400, "Invalid JSON request body")
		return jsonify({}), 201
	elif request.method == "GET":
		d = {}
		try:
			source = request.args['source']
			destination = request.args['destination']
		except KeyError:
			abort(400, "Invalid JSON request body")
		# print("8=D\n"*10, source, destination, areaPairRecorded(source, destination))
		if areaPairRecorded(source, destination):
			d = getRides(source, destination)
			# print("8====D\n", d)
			return jsonify(d)
		else:
			return jsonify(d), 204
	else:
		abort(405, "I don't appect this method")


# 5, 6, 7
@app.route("/api/v1/rides/<rideId>", methods=HTTP_METHODS)
def ride_details(rideId):
	addCount()
	if request.method == "GET":			# Get ride details
		# print("HERE")
		if rideExists(rideId):
			d = rideDetails(rideId)
			# print(d)
			# print("DONE")
			return jsonify(d)
		else:
			return jsonify({}), 204


	elif request.method == "POST":		# Join a ride
		try:
			join_user = request.get_json()["username"]
			if not userExists(join_user):
				abort(400, "User does not exist")
			elif not rideExists(rideId):
				abort(400, "Ride does not exist")
		except KeyError:
			abort(400, "Invalid JSON request body")
		joinRide(rideId, join_user)
		return jsonify({}), 200



	elif request.method == "DELETE":	# Delete a ride
		if rideExists(rideId):
			removeRide(rideId)
			return jsonify({}), 200
		else:
			abort(400, "Ride does not exist")
	else:
		abort(405, "I don't accept this method")


# Addition
@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
	conn = engine.connect()
	conn.execute("DELETE FROM RideDetails")
	return jsonify({})


# 8
@app.route('/api/v1/db/write', methods=["POST"])
def write_db():
	# print("in db write")
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
			# print(".\n"*5 + query)
			conn.execute(query)
			# res = conn.execute("SELECT * FROM " + table)
			# print(list(res))
			#conn.commit()
			#conn.close()

		elif action == "update":
			conn = engine.connect()
			query = "UPDATE " + table + " SET " + columns[0] + "='" + values + "' WHERE " + condition
			# print(".\n"*5 + query)
			conn.execute(query)

			#res = conn.execute("SELECT * FROM " + table)
			# conn.commit()
			# conn.close()
			# print(list(res))

		elif action == "delete":
			conn = engine.connect()
			query = "DELETE FROM "+ table + " WHERE " + condition
			# print(query)
			conn.execute(query)

			# res = conn.execute("SELECT * FROM " + table)
			# print(list(res))
			return jsonify({}), 200
			# conn.commit()
			# conn.close()
		return "OK"
	except KeyError:
		abort(400)

# 9
@app.route('/api/v1/db/read', methods=["POST"])
def read_db():
	# print("\tHERE")
	queryData = request.get_json()
	# print("\tAND HERE")
	# print(queryData)
	try:
		table = queryData['table']
		columns = queryData['columns']
		condition = queryData['where']
		# print(".\n"*10)
		conn = engine.connect()
		# res = conn.execute("SELECT * FROM RideDetails")
		# res = conn.execute("SELECT ride_id,created_by FROM RideDetails")
		query = "SELECT " + ",".join(columns) + " FROM " + table
		if condition:
			query += " WHERE " + condition
		# print(".\n" * 5, query)
		res = conn.execute(query)
		#conn.close()
		res = list(res)
		for index, _ in enumerate(res):
			res[index] = tuple(res[index])
		# print(res)
		# conn.close()
		return json.dumps(res)
	except KeyError:
		abort(400)


@app.route("/api/v1/rides/count", methods=["GET"])
def countRides():
	addCount()
	res = getRidesCount()
	return jsonify(res)


@app.route('/api/v1/_count', methods=["GET", "DELETE"])
def countCalls():
	#global API_COUNT
	if request.method == "GET":
		l = []
		#l.append(API_COUNT)
		f = open("count.txt", 'r')
		d = f.readlines()[0]
		f.close()
		l.append(int(d))
		return jsonify(l)
	elif request.method == "DELETE":
		#API_COUNT = 0
		f = open('count.txt', 'w')
		f.write('0')
		f.close()
		return jsonify([])


if __name__ == '__main__':
	# logging.basicConfig(filename='error.log',level=logging.DEBUG)
	app.debug=True
	app.run(host='0.0.0.0')
