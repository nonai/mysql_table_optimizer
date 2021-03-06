#!/usr/bin/python

import logging
import time
import MySQLdb
import re
import sys
import fileinput
import argparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler('/var/log/mysql-optimizer.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

#parsing arguments
parser = argparse.ArgumentParser(description='Script to optimize a list of MySQL tables to retain unused disk space',
	                                epilog='It takes care of rep lag as well as it start optimizing tables on small-big table order')
parser.add_argument('-f','--file', help='The file which contains the list of table in DB.TABLENAME format',required=True)
args = parser.parse_args()
file = args.file

#Check if the lag is 0 and if there is any Optmize process running

def check(server):
	db = MySQLdb.connect(server,"admin","*****","mysql")
        cursor = db.cursor()
	sql = "SHOW SLAVE STATUS"
	cursor.execute(sql)
	result = cursor.fetchall()
	db.close()
	if int(result[0][32]) == 0:
		db1 = MySQLdb.connect(server,"admin","****","mysql")
		cursor1 = db1.cursor()
		getQuer = "select INFO from information_schema.processlist"
		cursor1.execute(getQuer)
		checkP = cursor1.fetchall()
		db1.close()
		for i in checkP:
			#iterate and check if each process for optimize process
			if re.findall(r'OPTIMIZE*', str(i[0])):
				return 1
				break #break the for loop

		return 0 #since the check for Optimize process is done.
	else:
		return 1 #keeping default


def run_optimize(t):
	db = MySQLdb.connect("localhost","admin","****","mysql")
	cursor = db.cursor()
	sql = "SET SESSION sql_log_bin = 0; OPTIMIZE TABLE %s" %t
	logger.info('Executing run_optimize() for table %s' %t)
	cursor.execute(sql)
	logger.info('Closing DB connection called by run_optimize() for table %s' %t)
	db.close()


def main():
	if check('localhost') == 0:
		print "Success"
		f = open(file, 'r')
		data = open(file).read()
		for table in f:
			if not re.findall(r'done=.*',str(table)):
				table = table.rstrip()
				logger.info('Optimize starting on table %s' %table)
				try:
					run_optimize(table)
					f2 = open(file, 'w')
					f2.write( re.sub("%s" %table,"done=%s" %table,data) )
					f2.close()
				except Exception,e:
					print str(e)
					logger.info('ERROR for table %s: %s' %(str(table), str(e) ))
				break
		f.close()
	else:
		print "Failed"
		logger.info('Waiting.')

if __name__ == '__main__':
	while 1:
		main()
		time.sleep(30)
