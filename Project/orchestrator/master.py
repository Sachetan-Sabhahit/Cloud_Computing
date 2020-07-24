''' 
THE MASTER:
	1.Database structure:
		Tables:
			1.UserDetails("username", "password")
			2.RideDetails("Ride_id","created_by","timestamp","source","destination","riders_list")  
	2. All the write requests are made to the master via RPC calls
	3. Used 2 channels,one for each writeq and syncq
	4. SQL Query is generated from the json received from orchestrator executed and sent to syncq.
'''

import pika
import json
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey, exc
from random import randint
import ast
from kazoo.client import KazooClient
import docker
import os


def getMyPID():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	myPID = cont.top()["Processes"][0][1]
	return int(myPID)

def getMyName():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	return cont.name

engine = sql.create_engine('sqlite:///database/RideShare.db', echo=True)
meta = sql.MetaData()
user_details = Table('UserDetails', meta, 
	Column('username', String, primary_key=True),
	Column('password', String),
	)
ride_details = Table('RideDetails', meta, 
	Column('ride_id', Integer, primary_key=True),
	Column('created_by', String),
	Column('source', String),
	Column('destination', String),
	Column('timestamp', String),
	Column('riders_list', String),
	)


meta.create_all(engine)

#connecting to the zookeeper server
zk = KazooClient(hosts='zoo:2182')
zk.start()

'''Master creates a subnode to the 'election' node. When master dies, all other slaves
	get a notification indicating the need for an election among them.'''
if zk.exists("/election"):
    zk.create("/election/mastercandidate"+str(getMyPID()), b'WORKER', ephemeral=True)
else:
    print("ELECTION NOT DECLARED...")

print("MASTER HERE!\n"*10)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))

#channel for syncq
channel2 = connection.channel()
channel2.exchange_declare(exchange='syncq', exchange_type='fanout')

# To write SQL query to the syncq
def syncq_send(query_data):
	channel2.basic_publish(exchange='syncq', routing_key='', body=query_data)
	print(" sent query ",query_data)

# To generate SQL query from json received from writeq 
def write_db(queryData):
	try:
		values = queryData['insert'] 
		columns = queryData['columns'] 
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
			try:
				conn.execute(query)
			except exc.IntegrityError:
				response = "Check Integrity"
			q = str(query)
			syncq_send(q)
			response = "DONE"

		elif action == "update":
			conn = engine.connect()
			query = "UPDATE " + table + " SET " + columns[0] + "='" + values + "' WHERE " + condition
		
			conn.execute(query)
			q = str(query)
			syncq_send(q)
			response = "DONE"
			

		elif action == "delete":
			conn = engine.connect()
			query = "DELETE FROM "+ table 
			if condition:
				query += " WHERE " + condition
			print(query)
			conn.execute(query)
			q = str(query)
			syncq_send(q)

			response = "DONE"
		
		else:
			response = "UNKNOWN action"
	except KeyError:
		response = "Please provide proper JSON request body"
	return response


# read_db is used to query the database: queryData is a dictionary 
def read_db(queryData):
	try:
		table = queryData['table']
		columns = queryData['columns']
		condition = queryData['where']
		conn = engine.connect()
		query = "SELECT " + ",".join(columns) + " FROM " + table
		if condition:
			query += " WHERE " + condition
		res = conn.execute(query)
		res = list(res)
		for index, _ in enumerate(res):
			res[index] = tuple(res[index])
		
		return json.dumps(res)
	except:
		abort(400, "Please provide proper JSON request body")


def on_request(ch, method, props, body):
    body = body.decode("utf-8")
    queryData = ast.literal_eval(body)
    response = write_db(queryData)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

#channel-1(for writeq)
channel = connection.channel()
channel.queue_declare(queue='writeQ')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='writeQ', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()         ###Listen to writeq
