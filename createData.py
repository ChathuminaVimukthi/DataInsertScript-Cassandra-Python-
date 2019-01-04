
from datetime import timedelta
import datetime
from random import randint
import cql

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
		minute = new_time.minute
		new_month = 0
		if month == 1:
			month = 12
			timestamp_arr.append(datetime.datetime(year-1,month,day,hours,minute).strftime('%s'))
			#print(datetime.datetime(year-1,month,day,hours,minute).strftime('%s'))
		else:
			month = new_time.month - 1
			timestamp_arr.append(datetime.datetime(year,month,day,hours,minute).strftime('%s'))
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

generateTime()
generateValues()




#https://stackoverflow.com/questions/13217434/insert-to-cassandra-from-python-using-cql
#https://pypi.org/project/cql/1.4.0/



