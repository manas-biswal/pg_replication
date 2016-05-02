#!/usr/bin/python
# -*- coding: utf-8 -*-
#The following program is belongs to Open Technology Group, NIC, Chennai

import psycopg2
import sys
import urllib
import urllib2
import datetime
import json
import cookielib
import logging
import os
#------------------- to start the script following data to be filled up for database-----------------
primarydb_ip = '10.163.14.166'
db_port = 5432
db_name = 'postgres'
db_user = 'postgres'
db_password = 'postgres_2013'
data_path = '/home/postgres/postgres924/data' # here postgres data path need to be given for monitoring storage
location = 'HYD'
#login credentials for publishing report into portal with url
username = 'admin'
password = 'admin123'
url = 'http://10.163.14.72/drupaltrg/xml_data/user/login'

#fill up mobile numbers for sending sms, separate the mobile numbers with comma
contacts = '8015698191'
sms_url = 'https://10.160.33.184/smsmonitoring/sendsms'
#-----------------------------------------------------------------------------------------------------

def cookie_session():
			
			data = 'username=' +username+'&password='+password+'&checklogin=1'
			cj = cookielib.CookieJar()
			opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj)) # cookie has been enabled
			try:
				usrchk = opener.open(url, data) # url with user login credential sent
        			content = usrchk.read()
			except IOError:
					print "Error fetching page http://10.163.14.72/drupaltrg\nExiting now.."
					sys.exit()
			return opener
def db_storage(path):
			st = os.statvfs(path)
    			free = st.f_bavail * st.f_frsize
    			total = st.f_blocks * st.f_frsize
			return (free,total)	
def  bytes2human(n):
    			symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    			prefix = {}
    			for i, s in enumerate(symbols):
        					prefix[s] = 1 << (i+1)*10
    			for s in reversed(symbols):
        					if n >= prefix[s]:
            						value = float(n) / prefix[s]
            						return '%.1f%s' % (value, s)
    			return "%sB" % n	
def publish_structured():
			opener = cookie_session()
                        url_publish = "http://10.163.14.72/drupaltrg/xml_data/xmlservice"
                        try:
                                 send = opener.open(url_publish,data_to_send)
                        except IOError:
                                        print "Error sending data\nExiting now.."
                                        sys.exit()
                        print "Structured report published in portal"
def publish_unstructured():
			now = datetime.datetime.now()
                	date_time = now.strftime("%Y-%m-%d %H:%M")
			opener = cookie_session()
                        url_publish = "http://10.163.14.72/drupaltrg/location_service/pgpublish/"
                        message = 'Replication Down,Hence SMS alert has been sent to mobiles'
                        info = 'logdata'+ '='+date_time+':-'+ message
			try:
                            send = opener.open(url_publish,info)
                        except IOError:
                            print "Error sending message\nExiting now.."
                            sys.exit()
                        print "report published in portal"



con1 = None
#con2 = None
def sendsmsalert():
		get_url = sms_url + '/' + contacts
		now = datetime.datetime.now().strftime('on%%20%d,%b%Y%%20at%%20%H:%M')
		message = '%20Postgres%20master%20streaming%20status%20is%20Down%20'
	        data= message + now + '%20at'+ location + '('+ primarydb_ip + ')'
        	final_url = get_url + '/' + data
        	req = urllib2.Request(final_url)
        	print 'Attempt to send data......'

        	try:
            		response = urllib2.urlopen(req)
           		response_url = response.geturl()
            		if response_url==final_url:
                    			print 'SMS sent!'
					publish_unstructured()
        	except urllib2.URLError, e:
			            	print 'Send failed !'
            				print e.reason

def storage_publish():
		f,t = db_storage(data_path) 
		free_space = bytes2human(f)
		total_space = bytes2human(t)
		storage_info = free_space + '/' + total_space
		return storage_info
try:
    con1 = psycopg2.connect(host=primarydb_ip, port=db_port, database=db_name, user=db_user, password=db_password) 
#    con2 = psycopg2.connect(host='10.163.14.167', port='5432', database='postgres', user='postgres', password='postgres_2013')
    cur1 = con1.cursor()
    cur1.execute('SELECT * from pg_stat_replication')
    rows = cur1.fetchall()
    if len(rows) == 1: # checking for any row in pg_stat_replication
		cur1.execute('SELECT pid, usesysid, usename, application_name, client_addr, client_hostname, client_port, backend_start, state, sent_location, write_location, flush_location, replay_location, sync_priority, sync_state from pg_stat_replication') 

                ver1 = cur1.fetchone()
                       # To collect the report about date,time and storage 
		storage_info = storage_publish()
		now = datetime.datetime.now()
		date_time = now.strftime("%Y-%m-%d %H:%M")
		process_begins = str (ver1[7])
    		jdata = json.dumps({"date_time":date_time, "primarydb_address":primarydb_ip,"pid":ver1[0], "username":ver1[2], "app_name":ver1[3], "backend_start":process_begins, "client_addr":ver1[4],"state":ver1[8],"sent_location":ver1[9], "write_location":ver1[10], "flush_location":ver1[11],"replay_location":ver1[12],"db_storage":storage_info})
    		print jdata
		data_to_send = 'pgdata'+'='+jdata
		publish_structured()
		
    else:       # will send alert when replication goes down
		sendsmsalert() 

except psycopg2.DatabaseError, e:
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    
    if con1:
        con1.close()
#    if con2:
#       con2.close()

        
 #pid | usesysid | usename | application_name | client_addr | client_hostname | client_port | backend_start | state | sent_location | w
#rite_location | flush_location | replay_location | sync_priority | sync_state
