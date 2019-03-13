"""
	FileName: FaxlogParser.py
	Author: Bruce Dailey Jr.
	Date: 2018-09-07
	Python Version: 3.6.6 64bit

"""

from csv import reader


class ParseLog:

	def __init__(self, log, move):
		#print("Create Instance ", str(log))
		self.log = log
		self.move = move

	def movelog(self):
		#print(" Current log: ", self.log)
		#print("Move location: ", self.move)
		if not self.move.exists():	# After data is parsed file is moved to a parsed folder.
			self.log.replace(self.move)		

	def parse_inbound(self):
		#print('Found the inbound file: ' + str(self.log))
			
		dfList=[]
		f = open(self.log, "r")
		contents = reader(f)
		
		header = True
		for row in contents:
		
			if header == True:
				header = False
				continue
			
			oid = int(row[0])	  ## oid
			ts = row[1]           ## timestamp
			st = row[2]	          ## state
			ri = row[3]	          ## remote_id
			ch = int(row[4])      ## channel
			ex = row[5]           ## extension
			rn = row[6]           ## recipient_name
			pg = int(row[7])      ## num_pages
			dr = row[8]           ## delivery_results
			cl = int(row[9])      ## call_log_oid
			au = row[10]          ## archive_users
			ad = row[11]          ## archive_dir
			af = row[12]          ## archive_filename
			ci = row[13]          ## caller_id
			cnu = row[14]         ## caller_number
			cna = row[15]         ## caller_name
			if len(row) < 17:     ## is_ip
				ip = 0
			else:
				ip = int(row[16])
			
			
			args = (oid,ts,st,ri,ch,ex,rn,pg,dr,cl,au,ad,af,ci,cnu,cna,ip)
	
		
			dfList.append(args)
		
		
		f.close()
		
		return dfList


	def parse_outbound(self):
		#print('Found the outbound file: ' + str(self.log))
		
		dfList=[]
		f = open(self.log, "r")
		contents = reader(f)
		
		header = True
		for row in contents:
		
			if header == True:
				header = False
				continue
				
			oid = int(row[0])		## oid
			ts = row[1]             ## timestamp
			fk = row[2]             ## faxkey
			ek = row[3]             ## entrykey
			un = row[4]             ## username
			at = row[5]             ## agent
			mt = int(row[6])        ## max_tries
			ti = int(row[7])        ## try_interval
			py = int(row[8])        ## priority
			ps = int(row[9])        ## pages
			cpe = row[10]           ## cover_page_enabled
			cpn = row[11]           ## cover_page_name
			rt = row[12]            ## receipt
			sn = row[13]            ## sender_name
			se = row[14]            ## sender_email
			sp = row[15]            ## sender_phone
			sf = row[16]            ## sender_fax
			so = row[17]            ## sender_org
			rf = row[18]            ## rcpt_fax
			rp = row[19]            ## rcpt_phone
			rn = row[20]            ## rcpt_name
			ro = row[21]            ## rcpt_org
			sub = row[22]           ## subject
			cs = row[23]            ## comments
			st = row[24]            ## state
			fc = row[25]            ## fsched_code
			hc = int(row[26])       ## hangup_code
			stime = row[27]         ## start_time
			ftime = row[28]         ## finish_time
			app = row[29]           ## approver
			apptime = row[30]       ## approval_time
			ra = row[31]            ## receipt_attachment
			pr = row[32]            ## print_receipt
			prp = row[33]           ## print_receipt_printer
			prf = row[34]           ## print_receipt_first
			fps = row[35]           ## fax_page_size
			
			
			
			args = (oid,ts,fk,ek,un,at,mt,ti,py,ps,cpe,cpn,rt,sn,se,sp,sf,so,rf,rp,rn,ro,sub,cs,st,fc,hc,stime,ftime,app,apptime,ra,pr,prp,prf,fps)
			dfList.append(args)
		
		f.close()

		return dfList
		


		
	def parse_call(self):
		#print('Found the calls file: ' + str(self.log))
			
		dfList=[]
		f = open(self.log, "r")
		contents = reader(f)
		
		header = True
		for row in contents:
			#print(len(row))
	
			if header == True:
				header = False
				continue
			
			oid = int(row[0])	  ## oid
			ts = row[1]           ## timestamp
			rf = row[2]	          ## rcpt_fax 
			dir = row[3]          ## direction
			ek = row[4]           ## entrykey 
			rid = row[5]          ## remote_id
			stat = row[6]         ## status
			chm = int(row[7])     ## channel_modem
			sz = row[8]           ## size
			pgs = int(row[9])     ## pages
			res = row[10]         ## resolution
			if row[11] == "":	  ## baud_rate
				bdr = 0
			else:
				bdr = int(row[11])    
			dc = row[12]          ## data_compression
			ec = row[13]          ## error_correction
			ct = row[14]          ## connect_time
			et = int(row[15]) 	  ## elapsed_time
			dtmf =row[16]		  ## all_dtmf_digits
			if len(row) == 23:
				rfm = row[20]     ## rcpt_fax_mod
				cnum = row[21]    ## caller_number
				cname = row[22]   ## caller_name
				isip = 0          ## is_ip
				mn = ""           ## modem_nr
				wd = ""           ## width
				ht = ""           ## height
				init = ""         ## init_time
				oh = ""           ## off_hook_time
				ml = ""           ## modem_label
				ch = ""           ## channel
				err = row[17]     ## error
				ser = row[18]     ## sub_error
				mod = row[19]     ## modulation
			else:
				rfm = row[17]	  ## rcpt_fax_mod
				cnum = row[18]    ## caller_number
				cname = row[19]   ## caller_name
				isip = row[20]    ## is_ip
				mn = row[21]      ## modem_nr
				wd = row[22]      ## width
				ht = row[23]      ## height
				init = row[24]    ## init_time
				oh = row[25]      ## off_hook_time
				ml = row[26]      ## modem_label
				ch = row[27]      ## channel
				err = row[28]     ## error
				ser = row[29]     ## sub_error
				mod = row[30]     ## modulation
	
			
			args = (oid,ts,rf,dir,ek,rid,stat,chm,sz,pgs,res,bdr,dc,ec,ct,et,dtmf,rfm,cnum,cname,isip,mn,wd,ht,init,oh,ml,ch,err,ser,mod)
			dfList.append(args)
		
		f.close()

		return dfList

