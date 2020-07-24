''' 
THE ORCHESTRATOR:
	1. Writeq as well as readq both are implemented as RPC system and default exchange.
	2. Data replication to the new slave is done by copying the master's database.
	3. Fault tolerance is taken care using zookeeper nodes.
	4. Description of each function is mentioned at the top of it and logic comments are besides the code.
'''


import pika
import uuid
from flask import Flask, render_template, jsonify, request,abort
import docker
import os
import tarfile
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from random import randint
import atexit
from kazoo.client import KazooClient
from io import BytesIO

#Connecting to the zookeeper server
zk = KazooClient(hosts='zoo:2182')
zk.start()

#Flag to identify if a worker was CREATED or DESTROYED
FIRSTFLAG = False


# This is the workers node where all slave workers make subnodes. This takes care of slave scalabity
if not zk.exists("/workers"):
    zk.create("/workers", b'HEAD SLAVE')


'''This i sthe election node. Both master and slave nodes make subnodes here. Any change 
    to this node triggers events in all workers. Slaves use this trigger to figure out who
    the next master is.'''
if not zk.exists("/election"):
    zk.create("/election", b'ELECTION')

'''Zookeeper watch function which gets trigerred when a slave is created or destroyed.
   When a slave is destroyed the function checks the number of slaves currently running 
   and if it is less than required number of slaves it brings up the required slaves.
   When a slave is created the function just returns'''
@zk.ChildrenWatch('/workers', send_event=True)
def watch_children(children, event):
    
    global FIRSTFLAG                                       #GLOBAL FLAG to check if a slave is created 
    if not FIRSTFLAG:
        FIRSTFLAG =True
        print("FLAG SET")
        return
    print("THE STALKER IS HERE >:)", "\n"*10)
    print("EVENT:", event, event.type)
    print(dir(event))
    print("CHILDREN LENGTH:", len(children))
    WIDTH = 20
    global global_count
    requiredSlaves = global_count
    print("req slaves = ",requiredSlaves)
    if(len(children)< requiredSlaves):		
        FIRSTFLAG = False
        createSlave()

# To get the PID of the master worker
def getMaster():
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()["Processes"][0][1]
			pid_list.append((pid, c))
	pid_list.sort(key=lambda x:x[0])
	return pid_list[0][1]

# To get the total number of read requests made to the orhestrator
def getRequestCount():
    global THE_COUNT
    return THE_COUNT


# To reset the read requests count to zero
def resetRequestCount():
    global THE_COUNT
    THE_COUNT = 0

# Function that will be triggered after every two minutes after receiving the first request.
def checkRequestCount():
    WIDTH = 20                                        #Scaling for every 20 read requests
    print("CHECKING COUNT...", "\n"*10)
    count = getRequestCount()
    print("Count:", count)
    count = (abs(int(count)-1) // WIDTH)+1
    global global_count
    global_count = count
    slaveCount = getNoOfSlaves()                      #checking if for scale up and down
    if slaveCount < 0:
        print("NEGETIVE COUNT!!!!!")
    diff = count - slaveCount
    if diff > 0:                                      #no of slaves is less than required slaves.Scaling up
        for i in range(diff):		
            global FIRSTFLAG		
            FIRSTFLAG = False                         #setting the create FLAG.Creating a slave worker container as per needs
            createSlave()           
    elif diff < 0:                                    #no of slave is more than required slave.Scaling down
        for i in range(abs(diff)):  
            deleteSlave()                             #killing extra slave containers
    else:
        print("NO change")                            #no of slaves required and present are same

    resetRequestCount()                               #resetting the read request couter to zero after the 2 min time interval



# Setting a timer which calls checkRequestCount method every 2 minutes
cron = BackgroundScheduler(daemon=True)  
cron.add_job(checkRequestCount,'interval',seconds=120)


client = docker.from_env()                            #connecting to docker daemon
countFile = "myCount.txt"

timerStart = False                                    #timer starts once a request is made to the orchestrator


# To get a new slave name which'll be assigned to a freshly created slave.
def slaveName():
    name = ""
    flag1 = False
    while not flag1:
        name = "slave"+str(randint(1, 10**3))
        flag2 = False
        for c in client.containers.list():
            if c.name == name:
                flag2 = True
                break
        if not flag2:
            flag1 = True
    print("SLAVE NAME:", name)
    return name


# To get the number of current slaves running 
def getNoOfSlaves():
    client = docker.from_env()
    count = 0
    for c in client.containers.list():
        if "slave" in c.name:
            count += 1
    return count-1 			############# -1 as one of them is master


# To remove a slave. Used while scaling down.
def deleteSlave():
    client = docker.from_env()
    for c in client.containers.list():
        if "slave" in c.name and c.name != "slave1":  ####Slave1 is the default slave and cannot be removed during scaling down
            c.stop()
            break
    print("SLAVE DELETED\n"*10)



'''To create a new slave. Getting a name for the new slave,
   taking the image of the default slave to create new slave,copying the master's db to the newly created slave'''
def createSlave():
    c_name = slaveName()
    client = docker.from_env()
    image_1 = client.images.get("theslave")
    print("GOT IMAGE")
    c1 = client.containers.run(image = image_1, command='python /code/slave.py', links = {'rmq':'rmq', 'zoo':'zoo'},detach = True ,
        network = 'orchestrator_default', name=c_name, volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'},
        '/usr/bin/docker': {'bind': '/usr/bin/docker', 'mode': 'rw'}})

    print("GOT CONTAINER")
    copy_master_db_to_new_slave(c_name)######## Function call to maintain data consistency(made use of get_archive and put_archive)
    print("SLAVE CREATED\n"*10)

# Initialising the read counter
def initCount():
    global THE_COUNT
    THE_COUNT = 0

# Incrementing the read counter
def addCount():
    global THE_COUNT
    THE_COUNT += 1


'''function to retriew master's database and copy it to the newly created slave's
   container using put_archive and get_archive methods of docker sdk'''
def copy_master_db_to_new_slave(name):
    cl = docker.from_env()
    cont = getMaster()
    stream, status = cont.get_archive("/code/database/RideShare.db")
    file_obj = BytesIO()
    for i in stream:
        file_obj.write(i)
    file_obj.seek(0)
    tar = tarfile.open(mode='r', fileobj=file_obj)
    text = tar.extractfile('RideShare.db')
    q = text.read()
    #print(q)
    f = open("/code/database/RideShare.db", "wb")
    f.write(q)
    f.close()

    dst = '/code/database/RideShare.db' 
    src = '/code/database/RideShare.db'
    container = client.containers.get(name)
    print("GOT CREATED CONTAINER")
    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    print("PUTTING ARCHIVE")
    container.put_archive(os.path.dirname(dst), data)
    print("PUT ARCHIVE")



# RABBITMQ: Class for building rpc system
class MyRPC(object):

    def __init__(self, type):
        self.rpc_type = type
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq',heartbeat = 0))  ### heartbeat is set to zero to disable it.

        self.channel = self.connection.channel()                  ### establising a connection 

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(                               ### It's executed when the request is received back from worker
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, details):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',                                          ### using a default exchange for rpc
            routing_key=self.rpc_type+'Q',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,                     #### used to name a callback queue.
                correlation_id=self.corr_id,                      #### match a response with a request
		content_type = "application/json",                        #### Used to describe the mime-type of the encoding
            ),
            body=str(details))
        while(self.response is None):
            self.connection.process_data_events()
        return (self.response)

write_rpc = MyRPC("write")                                       ###rpc object for writeq
read_rpc = MyRPC("read")                                         ###rpc object for readq		
app=Flask(__name__)

print(" [x] Request Sent")                                        #sanity check
@app.route("/")
def greet():
	return "Hi there!"

# Wrtie db api
@app.route("/api/v1/db/write", methods=["POST"])
def write_deb():
    details = request.get_json()
    response = write_rpc.call(details)
    return response.decode("utf-8")

# Read DB api
@app.route("/api/v1/db/read", methods=["POST"])
def read_deb():
    global cron
    global timerStart
    if not timerStart:
        timerStart = True
        cron.start()
    addCount()
    details = request.get_json()
    response = read_rpc.call(details)
    return response.decode("utf-8")


# API to get the PID workers list
@app.route("/api/v1/worker/list",methods=["GET"])
def list_worker():
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if not (c.name =="rmq" or c.name =="orchestrator" or c.name == "zoo"):
			pid = c.top()['Processes'][0][1]
			pid_list.append(int(pid))
	pid_list.sort()
	return str(pid_list)


# API to crash the slave
@app.route("/api/v1/crash/slave",methods=["POST"])
def slavecrash():
	client = docker.from_env()
	pid_list = []
	c_list=[]
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()['Processes'][0][1]
			pid_list.append(pid)
			c_list.append(c)
	pid_list.sort()
	pid_to_kill = pid_list[-1]                ######## Killing the slave with highest PID
	for c in c_list:
		if(c.top()['Processes'][0][1] == pid_to_kill):
			c.kill()
			print("KILLED SLAVE")
			break
	return str(pid_to_kill)                   ######### returns the pid of the killed contanier


# API to kill the master container
@app.route("/api/v1/crash/master", methods=["POST"])
def mastercrash():
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()["Processes"][0][1]
			pid_list.append((pid, c))
	pid_list.sort(key=lambda x:x[0])
	pid_list[0][1].kill()
	print("MASTER ASSASINATED!\n"*10)
	createSlave()
	return "FINISH..."

# API to return the read request count
@app.route("/api/v1/_count", methods=["GET"])
def getCount():
    return THE_COUNT


# clear db: 2 queries for 2 tables .
@app.route("/api/v1/db/clear",methods = ["POST"])
def clearDb():
	requests.post("http://0.0.0.0:80/api/v1/db/write",json={"table":"UserDetails","insert":"","columns":"","action":"delete", "where":""})
	requests.post("http://0.0.0.0:80/api/v1/db/write",json={"table":"RideDetails","insert":"","columns":"","action":"delete", "where":""})
	return {}


# TESTING purpose only
@app.route("/my/test/workers", methods=["POST"])
def listworkers():
	return str(zk.get_children('/workers'))


if __name__ == '__main__':
	app.debug=True
	initCount()                           #initialiasing the read_count.
	global global_count
	global_count = 1                      #count that specifies the number of slaves required in the current 2 min cycle.Initially 1.
	app.run(host="0.0.0.0", port = 80, use_reloader=False)

