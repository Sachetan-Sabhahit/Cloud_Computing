from flask import Flask, render_template, jsonify, request,abort
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey
import requests
import json
from random import randint
import csv
from datetime import datetime as dt
import re


#server = '10.20.200.163'
server = "localhost"
user_port = '8080'
ride_port = '8000'

user_db_write_url = "http://" + server + ":" + user_port + "/api/v1/db/write"
user_db_read_url = "http://" + server + ":" + user_port + "/api/v1/db/read"

ride_db_write_url = "http://" + server + ":" + ride_port + "/api/v1/db/write"
ride_db_read_url = "http://" + server + ":" + ride_port + "/api/v1/db/read"

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
	r = requests.post(user_db_read_url, json={"table":"UserDetails", "columns":["username"], "where":"username='" + name + "'"})
	print("."*10, r)
	r = r.json()
	if r:
		return True
	return False

# To check if time is not valid
def wrongTime(time):
	if re.search("^\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d$", time):
		return False
	return True

# To add a ride
def addRide(created_by, timestamp, source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["ride_id"],
										'where':""})
	print(r.json())
	ride_ids = r.json()
	while True:
		new_ride_id = randint(0, 1000)
		if new_ride_id not in ride_ids:
			break

	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	print(list(res))
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "column":["ride_id", "created_by", "timestamp",
											"source", "destination", "riders_list"], 
											'insert':[new_ride_id, created_by, timestamp, source, destination,
														created_by+','], "action":"insert", "where":""})

	print("HERE:", r.status_code)
	res = conn.execute("SELECT * FROM RideDetails")
	print("Finally:", list(res))

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
											"where":""})
	r = r.json()
	d = []
	for i  in r:
		dt_object1 = dt.strptime(i[2], "%d-%m-%Y:%S-%M-%H")
		if dt_object1 > dt.now():
			d.append({"rideId":i[0], "username": i[1] , "timestamp":i[2]})
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
	print("UPDATED:", updated_riders_list)

	r = requests.post(ride_db_write_url, json={"table":"RideDetails",
										"column":["riders_list"], "action":"update",
										 "where":"ride_id="+ str(ride_id), "insert":updated_riders_list})
	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	print(list(res))

# To remove a ride
def removeRide(ride_id):
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "column":[], "action":"delete",
											"where":"ride_id='" + ride_id + "'", "insert":""})

	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	print(list(res))
	
# To check if it is a valid area
def validAreas(l):
	print("*\n"*10, "YO")
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
	print("IN USER_IN_RIDE:")
	print(r)
	for i in r:
		i = i[0].split(",")[:-1]
		if username in i:
			return True
	return False

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


# 3
@app.route("/api/v1/rides", methods=["POST"])
def create_new_ride():
	bad_request = False
	rideinfo = request.get_json()
	try:
		created_by = rideinfo["created_by"]
		timestamp = rideinfo["timestamp"]
		source = int(rideinfo["source"])
		destination = int(rideinfo["destination"])
		if not validAreas([source, destination]) or not userExists(created_by) or wrongTime(timestamp):
			print("HOLA")
			bad_request = True
		else:
			addRide(created_by, timestamp, source, destination)
	except KeyError:
		bad_request = True
	if bad_request:
		abort(400)
	else:
		return jsonify({}), 201


# 4
@app.route("/api/v1/rides", methods=["GET"])
def upcoming_rides():
	d = {}
	try:
		source = request.args['source']
		destination = request.args['destination']
	except:
		abort(400)
	if areaPairRecorded(source, destination):
		d = getRides(source, destination)
		return jsonify(d)
	else:
		return jsonify(d), 204


# 5, 6, 7
@app.route("/api/v1/rides/<rideId>", methods=["GET", "POST", "DELETE"])
def ride_details(rideId):
	if request.method == "GET":			# Get ride details
		print("HERE")
		if rideExists(rideId):
			d = rideDetails(rideId)
			print(d)
			print("DONE")
			return jsonify(d)
		else:
			return jsonify({}), 204


	elif request.method == "POST":		# Join a ride
		bad_request = False
		try:
			join_user = request.get_json()["username"]
			if not userExists(join_user) or not rideExists(rideId):
				bad_request = True
		except KeyError:
			bad_request = True
		if bad_request:
			return abort(400)
		else:
			joinRide(rideId, join_user)
			return jsonify({}), 200



	elif request.method == "DELETE":	# Delete a ride
		if rideExists(rideId):
			removeRide(rideId)
			return jsonify({}), 200
		else:
			abort(400)
			

# Addition
@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
	conn = engine.connect()
	conn.execute("DROP TABLE RideDetails")
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
		abort(400)

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
		abort(400)


		
if __name__ == '__main__':	
	app.debug=True
	app.run(host='0.0.0.0', port='8000')
