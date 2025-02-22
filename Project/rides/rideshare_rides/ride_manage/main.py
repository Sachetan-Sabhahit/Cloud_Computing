from flask import Flask, render_template, jsonify, request,abort
import requests
import json
from random import randint
import csv
from datetime import datetime as dt
import re
import logging


HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

user_server = "18.235.245.250"
ride_server = "18.235.245.250"

load_balancer = "rideshare-1090105330.us-east-1.elb.amazonaws.com"

user_port = '80'
ride_port = '80'

user_db_write_url = "http://" + user_server + ":" + user_port + "/api/v1/db/write"
user_db_read_url = "http://" + user_server + ":" + user_port + "/api/v1/db/read"

ride_db_write_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/write"
ride_db_read_url = "http://" + ride_server + ":" + ride_port + "/api/v1/db/read"

#To take care of 405 requests
def dummyRequest():
	r = requests.post(ride_db_read_url, json={"table":"UserDetails", "columns":["*"], "where":""})


def userExists(name):
	r = requests.get("http://"+load_balancer+"/api/v1/users", headers={"Origin":"35.168.94.255"})
	r = r.json()
	if name in r:
		return True
	return False

def wrongTime(time):
	if re.search("^\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d$", time):
		return False
	return True

def addRide(created_by, timestamp, source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["ride_id"],
										'where':""})
	ride_ids = r.json()
	while True:
		new_ride_id = randint(0, 100000000)
		if new_ride_id not in ride_ids:
			break
	
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "columns":["ride_id", "created_by", "timestamp",
											"source", "destination", "riders_list"], 
											'insert':[new_ride_id, created_by, timestamp, source, destination,
														created_by+','], "action":"insert", "where":""})


def areaPairRecorded(source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["source", "destination"],
											 "where":""})
	for i in r.json():
		if i[0] == source and i[1] == destination:
			return True
	return False



def getRides(source, destination):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
						"columns":["ride_id", "created_by", "timestamp"],
						"where":"source="+str(source)+" AND destination="+str(destination)})
	r = r.json()
	d = []
	for i  in r:
		dt_object1 = dt.strptime(i[2], "%d-%m-%Y:%S-%M-%H")
		if dt_object1 > dt.now():
			d.append({"rideId":i[0], "username": i[1] , "timestamp":i[2]})
	return d

def rideExists(rideId):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["ride_id"], 
											"where":"ride_id="+rideId})
	r = r.json()
	if len(r) > 0:
		return True
	return False

def rideDetails(ride_id):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
											"columns":["ride_id", "created_by", "riders_list", "timestamp",
														"source", "destination"],
											"where":"ride_id=" + ride_id})
	r = r.json()
	d = {"rideId":r[0][0], "Created_by":r[0][1], "users":r[0][2].split(',')[:-1],
			"Timestamp":r[0][3], "source":r[0][4], "destination":r[0][5]}
	return d


def joinRide(ride_id, username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails",
											"columns":["riders_list"],"where":"ride_id=" + ride_id})
	riders_list = r.json()[0][0]
	updated_riders_list = riders_list + username + ","
	r = requests.post(ride_db_write_url, json={"table":"RideDetails",
										"columns":["riders_list"], "action":"update",
										 "where":"ride_id="+ str(ride_id), "insert":updated_riders_list})
	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")
	


def removeRide(ride_id):
	r = requests.post(ride_db_write_url, json={"table":"RideDetails", "columns":[], "action":"delete",
											"where":"ride_id='" + ride_id + "'", "insert":""})

	conn = engine.connect()
	res = conn.execute("SELECT * FROM RideDetails")


def validAreas(l):
	f = open("AreaNameEnum.csv", 'r')
	data = list(csv.reader(f))
	f.close()
	number, area = zip(*data)
	for i in l:
		if str(i) not in number:
			return False
	return True

def userInRide(username):
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["riders_list"], "where":""})
	r = r.json()
	for i in r:
		i = i[0].split(",")[:-1]
		if username in i:
			return True
	return False


def getRidesCount():
	r = requests.post(ride_db_read_url, json={"table":"RideDetails", "columns":["COUNT(*)"], "where":""})
	res = r.json()
	return res[0]


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
		
		if areaPairRecorded(source, destination):
			d = getRides(source, destination)
		
			return jsonify(d)
		else:
			dummyRequest()
			return jsonify(d), 204
	else:
		dummyRequest()
		abort(405, "I don't appect this method")


# 5, 6, 7
@app.route("/api/v1/rides/<rideId>", methods=HTTP_METHODS)
def ride_details(rideId):
	addCount()
	if request.method == "GET":			# Get ride details
		
		if rideExists(rideId):
			d = rideDetails(rideId)
			return jsonify(d)
		else:
			dummyRequest()
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
		dummyRequest()
		abort(405, "I don't accept this method")




@app.route("/api/v1/rides/count", methods=["GET"])
def countRides():
	addCount()
	res = getRidesCount()
	return jsonify(res)


@app.route('/api/v1/_count', methods=["GET", "DELETE"])
def countCalls():
	if request.method == "GET":
		l = []
		f = open("count.txt", 'r')
		d = f.readlines()[0]
		f.close()
		l.append(int(d))
		return jsonify(l)
	elif request.method == "DELETE":
		f = open('count.txt', 'w')
		f.write('0')
		f.close()
		return jsonify([])


if __name__ == '__main__':
	app.debug=True
	app.run(host='0.0.0.0')
