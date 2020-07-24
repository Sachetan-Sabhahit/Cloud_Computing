'''
THE SLAVE:
	1. All the read requests are made to the slave via RPC calls
	2. Syncq is implemented using the fanout exchange and slave containe retriews the sql query 
	  from Syncq and executes the sql query to maintain data consistency.
	3. Database is created on startup.
	4. Database structure:
		Tables:
			1.UserDetails("username", "password")
			2.RideDetails("Ride_id","created_by","timestamp","source","destination","riders_list")  
	5. Used 2 channels,one for each readq and syncq
'''


import pika
import json
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from random import randint
import ast
from kazoo.client import KazooClient
import docker
import os
import sys

# Function to get the PID of all the worker containers 
def getAllWorkersPID():
	client = docker.from_env()
	pid_list = []
	c = client.containers.list()
	for c in client.containers.list():
		
		if 'slave' in c.name:
			pid = c.top()['Processes'][0][1]
			pid_list.append(int(pid))
	pid_list.sort()
	return pid_list

# Function to get the self PID 
def getMyPID():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	myPID = cont.top()['Processes'][0][1]
	return int(myPID)


# Function to get the name of the slave
def getMyName():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	return cont.name


'''This helper function decides whether the current container is the master.
	This is done by checking all slaves PIDs against its own PID and deciding 
	if it has the lowest'''
def iAmTheMaster():
	allPID = getAllWorkersPID()
	myPID = getMyPID()
	if myPID == allPID[0]:
		print("I'm the master !\n"*10)
		return True
	else:
		print(type(myPID), type(allPID[0]))


#Connecting to the zookeeper server
zk = KazooClient(hosts='zoo:2182')
zk.start()

'''Slaves create this subnode under 'worker' to which the orchestrator listens
	to. This facilitates auto scalability'''
if zk.exists("/workers"):
    zk.create("/workers/worker"+str(getMyPID())+":::"+getMyName(), b'WORKER', ephemeral=True)
else:
    print("WORKER HEAD IS MISSING...")

'''Slaves take part in the election process by creating subnodes under 'election' node.'''
if zk.exists("/election"):
    zk.create("/election/candidate"+str(getMyPID()), b'CANDIDATE', ephemeral=True)
else:
    print("ELECTION NOT DECLARED...")


'''Slaves watch the election node, which gets triggered when a master dies(among other triggers).
	On this event, the slaves perform a check if they have the current lowest PID; if so, they go
	the master.'''
@zk.ChildrenWatch("/election")
def watch_children(children):
    print("MASTER MUST BE GONE...\n"*10)
    if iAmTheMaster():
        os.execl(sys.executable, 'python3', 'master.py')
        exit(0)



# DATABASE Creation
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

# Writing into the database: queryData is a dictionary which has keys= ("insert","columns","table","action","where")
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
			conn.execute(query)
			response = "DONE"

		elif action == "update":
			conn = engine.connect()
			query = "UPDATE " + table + " SET " + columns[0] + "='" + values + "' WHERE " + condition
			# print(".\n"*5 + query)
			conn.execute(query)
			response = "DONE"
			

		elif action == "delete":
			conn = engine.connect()
			query = "DELETE FROM "+ table + " WHERE " + condition
			# print(query)
			conn.execute(query)
			response = "DONE"
		
		else:
			response = "UNKNOWN action"
	except KeyError:
		response = "Please provide proper JSON request body"
	return response


# read_db is used to query the databse: queryData is a dictionary
def read_db(queryData):
	try:
		table = queryData['table']
		columns = queryData['columns']
		condition = queryData['where']
		conn = engine.connect()
		query = "SELECT " + ",".join(columns) + " FROM " + table
		if condition:
			query += " WHERE " + condition
		print(".\n" * 5, query)           ###query is created
		res = conn.execute(query)
		res = list(res)
		for index, _ in enumerate(res):
			res[index] = tuple(res[index])
		response = json.dumps(res)
	except Exception as e:
		print("Error is:", e)
		response = "Please provide proper JSON request body"
	return response


''' method to run when receive a message from readq.
	Sends an acknoweldgement(about received message from readq) only after completing the read and sending it back to make sure read
	requests are not lost '''
def on_request(ch, method, props, body):
    body = body.decode("utf-8")
    print(body, "\n"*10)
    queryData = ast.literal_eval(body)
    print(queryData, type(queryData), "\n"*10)
    response = read_db(queryData)                                    ### calls the read_db function.Passes the json to it.
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))                             ### response from read_db is sent back to the orchestrartor
    ch.basic_ack(delivery_tag=method.delivery_tag)                   #acknoledgement



connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()                                       ### Channel for readq
channel.queue_declare(queue='readQ')
channel.basic_qos(prefetch_count=1)                                  ###spread the load equally over multiple slaves
channel.basic_consume(queue='readQ', on_message_callback=on_request) #on_request method executed when the request is received from readq

#CHANNEL for syncq
channel2 = connection.channel()
channel2.exchange_declare(exchange='syncq', exchange_type='fanout') #used fanout exchange(Boradcasting)
result = channel2.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel2.queue_bind(exchange='syncq', queue=queue_name)
def callback(ch, method, properties, body):                         ###callback function called when the request is received from syncq
	print("received ",body.decode("utf-8"))           
	query = body.decode("utf-8")              
	conn = engine.connect()                                     ### receiving the sql query from the syncq and executing it
	conn.execute(query)

	
channel2.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel2.start_consuming()                                          ### lsitening to syncq
channel.start_consuming()                                           ### listening to readq
