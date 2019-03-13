"""
	FileName: FaxlogParser.py
	Author: Bruce Dailey Jr.
	Date: 2018-09-07
	Python Version: 3.6.6 64bit
	
	Description: This script is designed to parse the fax logs that are generated from the Multi-Tech
	fax gateway. The logs are configured to export from the gateway on a dailey basis, and export the previous days logs.
	The fax Gateway is currently configured to export the logs to "\\fileserver\fax\expired entries"
	The log data is input to a MySQL DB (SCHC-DB02) via stored procedure. A table is setup for each log type (Inbound, Outbound, Call).

	2019-03-05 - Moved parsing functions to a new file (parser.py) and created the Parse class. The log and move variables
					are now attributes of the class and each parse function has been made a method.

"""


import mysql.connector as mysql
from logparser import ParseLog
from os import environ
from pathlib import Path, PurePath
from re import compile, match
from datetime import datetime

now = datetime.now()
dtfmt =  now.strftime("%Y-%m-%d %H:%M")

#print("Current directory is : " + str(Path.cwd()))
fLog = open(Path.cwd() / "FaxLogParser.txt", 'a')

def import_log(procedure, data):	# Import data into MySQL DB faxlogs_db

	uname =  environ.get("PY_DB_User")
	pword = environ.get("PY_DB_Pass")

	i = 0			# Record counter
	counter = 0		# Total record counter

	mydb = mysql.connect(host="schc-db02", user=uname, passwd=pword, database="faxlogs_db")	# Connect to DB
	mycursor = mydb.cursor()	# Set db cursor
	
	for line in data:
		
		mycursor.callproc(procedure, line)		# call passed stored procedure for current row of data
			
		i += 1
		counter += 1
		
		if  i >= 1000:		# Commit every 1000 records to limit resource use on server
			mydb.commit()
			i = 0
			fLog.write("Committing current 1000 records. Total records: " + str(counter))
	
	mydb.commit()
	fLog.write("\t\t\tCommiting final: " + str(i) + " records.\n")
	mycursor.close()
	mydb.close()
	fLog.write("\t\t\t" + str(counter) + " records have been parsed\n\n")




def parse_files(list, folder):	# Detrmine which log file type and send to correct parsing routine.


	logtype = compile(r'faxlog.(\w+)')	# Regex to match log type.

	for file in list:
		#print(file)
		fileNames = PurePath(file).parts[-1]	# Returns just the name of the file, regex crashes if file path is included.
		#print(fileNames)
		ftype = match(logtype, fileNames)	# Looks at filename and matches text	
		#print(ftype.group(1))
		#inbound = parser.Parse(file, Path(folder / "Incoming Parsed" / fileNames))
		try:
			if ftype.group(1) == 'inbound':
				#print("Inbound Selected")
				inbound = ParseLog(file, Path(folder / "Incoming Parsed" / fileNames))  # Create class instance and pass file and move path for inbound log.
				fLog.write(dtfmt + "\tImporting Incoming log file: " + str(fileNames) + "\n")
				import_log('sp_incoming_logs', inbound.parse_inbound())  # Call database function with stored procedure and parsed log data.
				#print(inbound.parse_inbound())
				inbound.movelog()  #  Move parsed file.
			
			elif ftype.group(1) == 'outbound':
				#print("Outbound Selected")
				outbound = ParseLog(file, Path(folder / "Outgoing Parsed" / fileNames)) # Create class instance and pass file and move path for outbound log.
				fLog.write(dtfmt + "\tImporting Outgoing log file: " + str(fileNames) + "\n")
				import_log('sp_outboundlogs_insert', outbound.parse_outbound())  # Call database function with stored procedure and parsed log data.
				#print(outbound.parse_outbound())
				outbound.movelog()  #  Move parsed file.
			
			elif ftype.group(1) == 'call':
				#print("Call Selected")
				call = ParseLog(file, Path(folder / "Call Parsed" / fileNames)) # Create class instance and pass file and move path for call log.
				fLog.write(dtfmt + "\tImporting Call log file: " + str(fileNames) + "\n")
				import_log('sp_calllogs_insert', call.parse_call())  # Call database function with stored procedure and parsed log data.
				#print(call.parse_call())
				call.movelog()  #  Move parsed file.
					
		except Exception as e:
			print(e)
			fLog.write(dtfmt + "\tNo matching file type for: " +  str(fileNames) + "\n")



if __name__ == "__main__":


	folder = Path("//schc-file01/Fax/Expired Entries")	# Location of log files.
	#print(pglob)
	#print(folder)
	#print(getFiles)
	fileList = list(folder.glob("*.csv"))	# Grab all files with CSV extention and create list.
	#print(fileList)
	
	parse_files(fileList, folder)
	
	
	fLog.close()
	
