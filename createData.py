#Script to insert data to cassandra(TAP Insights)
#Env - Python 3
#Extra modules - Cassandra Driver
#Installation - pip3 install cassandra-driver

from datetime import timedelta
import datetime
from random import randint
from cassandra.cluster import Cluster

#timestamp array and values array
timestamp_arr = []
values_arr = []
#Application level time series ids
app_level_rev_arr = [
	"application.APP_0001.revenue",
	"application.APP_0001.revenue.sms",
	"application.APP_0001.revenue.ussd",
	"application.APP_0001.revenue.subscription",
	"application.APP_0001.revenue.cas",
	"application.APP_0001.revenue.wap-push"
]
app_level_traffic_arr = [
	"application.APP_0001.traffic",
	"application.APP_0001.traffic.sms",
	"application.APP_0001.traffic.ussd",
	"application.APP_0001.traffic.subscription",
	"application.APP_0001.traffic.cas",
	"application.APP_0001.traffic.wap-push"
]
app_level_subs_arr = [
	"application.APP_0001.subscribers",
	"application.APP_0001.subscribers-registered",
	"application.APP_0001.subscribers-canceled",
	"application.APP_0001.subscribers-active",
	"application.APP_0001.subscribers-inactive"
]

#Method to generate timetamps
def generateTime():
	i = 0
	dataAmount = 1000
	start_time = datetime.datetime.now()

	while i < dataAmount:
		i = i + 1
		new_time = start_time + timedelta(hours=1)
		year = new_time.year
		month = new_time.month
		day = new_time.day
		hours = new_time.hour
		minute = 30
		seconds = 00
		miliseconds = 00
		new_month = 0
		if month == 1:
			month = 12
			timestamp_arr.append(datetime.datetime(year-1,month,day,hours,minute,seconds,miliseconds).strftime('%s'))
			#print(datetime.datetime(year-1,month,day,hours,minute).strftime('%s'))
		else:
			month = new_time.month - 1
			timestamp_arr.append(datetime.datetime(year,month,day,hours,minute,seconds,miliseconds).strftime('%s'))
			#print(datetime.datetime(year,month,day,hours,minute).strftime('%s'))
		start_time = new_time
	#print(timestamp_arr)
	return

#Method to generate values
def generateValues():
	for x in range(1000):
		values_arr.append(randint(50, 200))
	#print(values_arr)
	return

def injectData(appId,timestamp,value):
	print("Connecting to Cassandra...")
	cluster = Cluster()
	session = cluster.connect('insights')
	session.execute("INSERT INTO time_series (id, time, value) VALUES (%s, %s, %s)",(appId,int(timestamp),value))
	return

generateTime()
generateValues()
injectData(app_level_rev_arr[0],timestamp_arr[0],values_arr[0])


#https://stackoverflow.com/questions/13217434/insert-to-cassandra-from-python-using-cql
#https://pypi.org/project/cql/1.4.0/
