# Cloud_Computing
RideShare application

For orchestrator VM :
--Navigate to Project/orchestrator/ folder and run(with sudo access):
  1)  docker build . -t theslave:latest
  2)  docker-compose up
--In the orchestrator.py file check for the network name.
--This will get the orchestrator up and running along with zookeeper rabbitmq and 2 workers.

For Users and Rides VM :
--Navigate to Project/users or Project/rides and (folder with docker-compose.yml file) run (assumming sudo access):
  docker-compose up 
--This will get the user and ride container running in their repective VMs.


THE ORCHESTRATOR:
	1)Writeq as well as readq both are implemented as RPC system and default exchange.
	2)Data replication to the new slave is done by copying the master's database.
	3)Fault tolerance is taken care using zookeeper nodes.
	4)Description of each function is mentioned at the top of it and logic comments are besides the code.

THE MASTER:
	1.database structure:
	 Tables:
		1.UserDetails("username", "password")
		2.RideDetails("Ride_id","created_by","timestamp","source","destination","riders_list")  
	2.All the write requests are made to the master via RPC calls
	3.Used 2 channels,one for each writeq and syncq
	4.SQL Query is generated from the json received from orchestrator executed and sent to syncq.
  
THE SLAVE:
	1.All the read requests are made to the slave via RPC calls
	2.Syncq is implemented using the fanout exchange and slave containe retriews the sql query 
	  from Syncq and executes the sql query to maintain data consistency.
	3.Database is created on startup.
	4.database structure:same as master
		Tables:
			1.UserDetails("username", "password")
			2.RideDetails("Ride_id","created_by","timestamp","source","destination","riders_list")  
	5.Used 2 channels,one for each readq and syncq
