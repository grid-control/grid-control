
"""
 * ApMon - Application Monitoring Tool
 * Version: 2.2.1
 *
 * Copyright (C) 2006 California Institute of Technology
 *
 * Permission is hereby granted, free of charge, to use, copy and modify 
 * this software and its documentation (the "Software") for any
 * purpose, provided that existing copyright notices are retained in 
 * all copies and that this notice is included verbatim in any distributions
 * or substantial portions of the Software. 
 * This software is a part of the MonALISA framework (http://monalisa.cacr.caltech.edu).
 * Users of the Software are asked to feed back problems, benefits,
 * and/or suggestions about the software to the MonALISA Development Team
 * (developers@monalisa.cern.ch). Support for this software - fixing of bugs,
 * incorporation of new features - is done on a best effort basis. All bug
 * fixes and enhancements will be made available under the same terms and
 * conditions as the original software,

 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES THEREOF,
 * EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. THIS SOFTWARE IS
 * PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE NO
 * OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
"""


import os
import re
import time
import string
import socket
import Logger

"""
Class ProcInfo
extracts information from the proc/ filesystem for system and job monitoring
"""
class ProcInfo:
	# ProcInfo constructor
	def __init__ (this, logger):
		this.DATA = {};             # monitored data that is going to be reported
		this.LAST_UPDATE_TIME = 0;  # when the last measurement was done
		this.JOBS = {};             # jobs that will be monitored
		this.logger = logger	    # use the given logger
		this.OS_TYPE = os.popen('uname -s').readline().replace('\n','');
	
	# This should be called from time to time to update the monitored data,
	# but not more often than once a second because of the resolution of time()
	def update (this):
		if this.LAST_UPDATE_TIME == int(time.time()):
			this.logger.log(Logger.NOTICE, "ProcInfo: update() called too often!");
			return;
		this.readStat();
		this.readMemInfo();
		if this.OS_TYPE == 'Darwin':
			this.darwin_readLoadAvg();
		else:
			this.readLoadAvg();
		this.countProcesses();
		this.readGenericInfo();
		this.readNetworkInfo();
		this.readNetStat();
		for pid in this.JOBS.keys():
			this.readJobInfo(pid);
			this.readJobDiskUsage(pid);
		this.LAST_UPDATE_TIME = int(time.time());
		this.DATA['TIME'] = int(time.time());
		
	# Call this to add another PID to be monitored
	def addJobToMonitor (this, pid, workDir):
		this.JOBS[pid] = {};
		this.JOBS[pid]['WORKDIR'] = workDir;
		this.JOBS[pid]['DATA'] = {};
		#print this.JOBS;
	
	# Call this to stop monitoring a PID
	def removeJobToMonitor (this, pid):
		if this.JOBS.has_key(pid):
			del this.JOBS[pid];

	# Return a filtered hash containting the system-related parameters and values
	def getSystemData (this, params, prevDataRef):
		return this.getFilteredData(this.DATA, params, prevDataRef);
	
	# Return a filtered hash containing the job-related parameters and values
	def getJobData (this, pid, params):
		if not this.JOBS.has_key(pid):
			return [];
		return this.getFilteredData(this.JOBS[pid]['DATA'], params);

	############################################################################################
	# internal functions for system monitoring
	############################################################################################
	
	# this has to be run twice (with the $lastUpdateTime updated) to get some useful results
	# the information about pages_in/out and swap_in/out isn't available for 2.6 kernels (yet)
	def readStat (this):
		try:
			FSTAT = open('/proc/stat');
			line = FSTAT.readline();
        	        while(line != ''):
				if(line.startswith("cpu ")):
					elem = re.split("\s+", line);
					this.DATA['raw_cpu_usr'] = float(elem[1]);
					this.DATA['raw_cpu_nice'] = float(elem[2]);
					this.DATA['raw_cpu_sys'] = float(elem[3]);
					this.DATA['raw_cpu_idle'] = float(elem[4]);
				if(line.startswith("page")):
					elem = line.split();
					this.DATA['raw_pages_in'] = float(elem[1]);
					this.DATA['raw_pages_out'] = float(elem[2]);
				if(line.startswith('swap')):
					elem = line.split();
					this.DATA['raw_swap_in'] = float(elem[1]);
					this.DATA['raw_swap_out'] = float(elem[2]);
				line = FSTAT.readline();
			FSTAT.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/stat");
			return;

	# sizes are reported in MB (except _usage that is in percent).
	def readMemInfo (this):
		try:
			FMEM = open('/proc/meminfo');
			line = FMEM.readline();
        	        while(line != ''):
				elem = re.split("\s+", line);
				if(line.startswith("MemFree:")):
					this.DATA['mem_free'] = float(elem[1]) / 1024.0;
				if(line.startswith("MemTotal:")):
					this.DATA['total_mem'] = float(elem[1]) / 1024.0;
				if(line.startswith("SwapFree:")):
					this.DATA['swap_free'] = float(elem[1]) / 1024.0;
				if(line.startswith("SwapTotal:")):
					this.DATA['total_swap'] = float(elem[1]) / 1024.0;
				line = FMEM.readline();
			FMEM.close();
			if this.DATA.has_key('total_mem') and this.DATA.has_key('mem_free'):
				this.DATA['mem_used'] = this.DATA['total_mem'] - this.DATA['mem_free'];
			if this.DATA.has_key('total_swap') and this.DATA.has_key('swap_free'):
				this.DATA['swap_used'] = this.DATA['total_swap'] - this.DATA['swap_free'];
			if this.DATA.has_key('mem_used') and this.DATA.has_key('total_mem') and this.DATA['total_mem'] > 0:
				this.DATA['mem_usage'] = 100.0 * this.DATA['mem_used'] / this.DATA['total_mem'];
			if this.DATA.has_key('swap_used') and this.DATA.has_key('total_swap') and this.DATA['total_swap'] > 0:
				this.DATA['swap_usage'] = 100.0 * this.DATA['swap_used'] / this.DATA['total_swap'];
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo");
			return;

	# read system load average
	def readLoadAvg (this):
		try:
			FAVG = open('/proc/loadavg');
			line = FAVG.readline();
			FAVG.close();
			elem = re.split("\s+", line);
			this.DATA['load1'] = float(elem[0]);
			this.DATA['load5'] = float(elem[1]);
			this.DATA['load15'] = float(elem[2]);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo");
			return;

	
	# read system load average on Darwin
	def darwin_readLoadAvg (this):
		try:
			LOAD_AVG = os.popen('sysctl vm.loadavg');
			line = LOAD_AVG.readline();
			LOAD_AVG.close();
			elem = re.split("\s+", line);
			this.DATA['load1'] = float(elem[1]);
			this.DATA['load5'] = float(elem[2]);
			this.DATA['load15'] = float(elem[3]);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot run 'sysctl vm.loadavg");
			return;


	# read the number of processes currently running on the system
	def countProcesses (this):
		"""
		# old version
		nr = 0;
		try:
			for file in os.listdir("/proc"):
				if re.match("\d+", file):
					nr += 1;
			this.DATA['processes'] = nr;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc to count processes");
			return;
		"""
		# new version
		total = 0;
		states = {'D':0, 'R':0, 'S':0, 'T':0, 'Z':0};
		try:
		    output = os.popen('ps -A -o state');
		    line = output.readline();
		    while(line != ''):
			states[line[0]] = states[line[0]] + 1;
			total = total + 1;
			line = output.readline();
		    output.close();
		    this.DATA['processes'] = total;
		    for key in states.keys():
			this.DATA['processes_'+key] = states[key];
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot get output from ps command");
			return;

	# reads the IP, hostname, cpu_MHz, uptime
	def readGenericInfo (this):
		this.DATA['hostname'] = socket.getfqdn();
		try:
			output = os.popen('/sbin/ifconfig -a')
			eth, ip = '', '';
			line = output.readline();
			while(line != ''):
				line = line.strip();
				if line.startswith("eth"):
					elem = line.split();
					eth = elem[0];
					ip = '';
				if len(eth) > 0 and line.startswith("inet addr:"):
					ip = re.match("inet addr:(\d+\.\d+\.\d+\.\d+)", line).group(1);
					this.DATA[eth + '_ip'] = ip;
					eth = '';
				line = output.readline();
			output.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot get output from /sbin/ifconfig -a");
			return;
		try:
			no_cpus = 0;
			FCPU = open('/proc/cpuinfo');
			line = FCPU.readline();
        	        while(line != ''):
				if line.startswith("cpu MHz"):
					this.DATA['cpu_MHz'] = float(re.match("cpu MHz\s+:\s+(\d+\.?\d*)", line).group(1));
					no_cpus += 1;
				
				if line.startswith("vendor_id"):
					this.DATA['cpu_vendor_id'] = re.match("vendor_id\s+:\s+(.+)", line).group(1);
					
				if line.startswith("cpu family"):
					this.DATA['cpu_family'] = re.match("cpu family\s+:\s+(.+)", line).group(1);
					
				if line.startswith("model") and not line.startswith("model name") :
					this.DATA['cpu_model'] = re.match("model\s+:\s+(.+)", line).group(1);
					
				if line.startswith("model name"):
					this.DATA['cpu_model_name'] = re.match("model name\s+:\s+(.+)", line).group(1);
					
				if line.startswith("bogomips"):
					this.DATA['bogomips'] = float(re.match("bogomips\s+:\s+(\d+\.?\d*)", line).group(1));
				
				line = FCPU.readline();
			FCPU.close();
			this.DATA['no_CPUs'] = no_cpus;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/cpuinfo");
			return;
		try:
			FUPT = open('/proc/uptime');
        	        line = FUPT.readline();
			FUPT.close();
			elem = line.split();
			this.DATA['uptime'] = float(elem[0]) / (24.0 * 3600);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/uptime");
			return;
	
	# do a difference with overflow check and repair
	# the counter is unsigned 32 or 64   bit
	def diffWithOverflowCheck(this, new, old):
		if new >= old:
			return new - old;
		else:
			max = (1L << 31) * 2;  # 32 bits
			if old >= max:
				max = (1L << 63) * 2;  # 64 bits
			return new - old + max;
	
	# read network information like transfered kBps and nr. of errors on each interface
	def readNetworkInfo (this):
		try:
			FNET = open('/proc/net/dev');
			line = FNET.readline();
			while(line != ''):
				m = re.match("\s*eth(\d):(\d+)\s+\d+\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+\d+\s+(\d+)", line);
				if m != None:
					this.DATA['raw_eth'+m.group(1)+'_in'] = float(m.group(2));
					this.DATA['raw_eth'+m.group(1)+'_out'] = float(m.group(4));
					this.DATA['raw_eth'+m.group(1)+'_errs'] = int(m.group(3)) + int(m.group(5));
				line = FNET.readline();
			FNET.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/net/dev");
			return;

	# run nestat and collect sockets info (tcp, udp, unix) and connection states for tcp sockets from netstat
	def readNetStat(this):
	    try:
		output = os.popen('netstat -an 2>/dev/null');
		sockets = { 'sockets_tcp':0, 'sockets_udp':0, 'sockets_unix':0, 'sockets_icm':0 };
		tcp_details = { 'sockets_tcp_ESTABLISHED':0, 'sockets_tcp_SYN_SENT':0, 
		    'sockets_tcp_SYN_RECV':0, 'sockets_tcp_FIN_WAIT1':0, 'sockets_tcp_FIN_WAIT2':0,
		    'sockets_tcp_TIME_WAIT':0, 'sockets_tcp_CLOSED':0, 'sockets_tcp_CLOSE_WAIT':0,
		    'sockets_tcp_LAST_ACK':0, 'sockets_tcp_LISTEN':0, 'sockets_tcp_CLOSING':0,
		    'sockets_tcp_UNKNOWN':0 };
		line = output.readline();
		while(line != ''):
		    arg = string.split(line);
		    proto = arg[0];
		    if proto.find('tcp') == 0:
			sockets['sockets_tcp'] += 1;
			state = arg[len(arg)-1];
			key = 'sockets_tcp_'+state;
			if tcp_details.has_key(key):
		    	    tcp_details[key] += 1;
		    if proto.find('udp') == 0:
                	sockets['sockets_udp'] += 1;
		    if proto.find('unix') == 0:
                	sockets['sockets_unix'] += 1;
		    if proto.find('icm') == 0:
                	sockets['sockets_icm'] += 1;

            	    line = output.readline();
        	output.close();

        	for key in sockets.keys():
            	    this.DATA[key] = sockets[key];
        	for key in tcp_details.keys():
            	    this.DATA[key] = tcp_details[key];
	    except IOError, ex:
                this.logger.log(Logger.ERROR, "ProcInfo: cannot get output from netstat command");
                return;

	##############################################################################################
	# job monitoring related functions
	##############################################################################################
	
	# internal function that gets the full list of children (pids) for a process (pid)
	def getChildren (this, parent):
		pidmap = {};
		try:
			output = os.popen('ps -A -o "pid ppid"');
			line = output.readline(); # skip headers
			line = output.readline();
			while(line != ''):
				line = line.strip();
				elem = re.split("\s+", line);
				pidmap[elem[0]] = elem[1];
				line = output.readline();
			output.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps -A -o \"pid ppid\"");

		if pidmap.has_key(parent):
			this.logger.log(Logger.INFO, 'ProcInfo: No job with pid='+str(parent));
			this.removeJobToMonitor(parent);
			return [];

		children = [parent];				
		i = 0;
		while(i < len(children)):
			prnt = children[i];
			for (pid, ppid) in pidmap.items():
				if ppid == prnt:
					children.append(pid);
        		i += 1;
		return children;

	# internal function that parses a time formatted like "days-hours:min:sec" and returns the corresponding
	# number of seconds.
	def parsePSTime (this, my_time):
		my_time = my_time.strip();
		m = re.match("(\d+)-(\d+):(\d+):(\d+)", my_time);
		if m != None:
			return int(m.group(1)) * 24 * 3600 + int(m.group(2)) * 3600 + int(m.group(3)) * 60 + int(m.group(4));
		else:
			m = re.match("(\d+):(\d+):(\d+)", my_time);
			if(m != None):
				return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3));
			else:
				m = re.match("(\d+):(\d+)", my_time);
				if(m != None):
					return int(m.group(1)) * 60 + int(m.group(2));
				else:
					return 0;

	# read information about this the JOB_PID process
	# memory sizes are given in KB
	def readJobInfo (this, pid):
		if (pid == '') or not this.JOBS.has_key(pid):
			return;
		children = this.getChildren(pid);
		if(len(children) == 0):
			this.logger.log(Logger.INFO, "ProcInfo: Job with pid="+str(pid)+" terminated; removing it from monitored jobs.");
			#print ":("
			this.removeJobToMonitor(pid);
			return;
		try:
			JSTATUS = os.popen("ps --no-headers --pid " + ",".join([`child` for child in  children]) + " -o pid,etime,time,%cpu,%mem,rsz,vsz,comm");
			mem_cmd_map = {};
			etime, cputime, pcpu, pmem, rsz, vsz, comm, fd = 0, 0, 0, 0, 0, 0, 0, 0;
			line = JSTATUS.readline();
			while(line != ''):
				line = line.strip();
				m = re.match("(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)", line);
				if m != None:
					apid, etime1, cputime1, pcpu1, pmem1, rsz1, vsz1, comm1 = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8);
					sec = this.parsePSTime(etime1);
					if sec > etime: 	# the elapsed time is the maximum of all elapsed
						etime = sec;
					sec = this.parsePSTime(cputime1); # times corespornding to all child processes.
					cputime += sec;	# total cputime is the sum of cputimes for all processes.
					pcpu += float(pcpu1); # total %cpu is the sum of all children %cpu.
					if not mem_cmd_map.has_key(`pmem1`+" "+`rsz1`+" "+`vsz1`+" "+`comm1`):
						# it's the first thread/process with this memory footprint; add it.
						mem_cmd_map[`pmem1`+" "+`rsz1`+" "+`vsz1`+" "+`comm1`] = 1;
						pmem += float(pmem1); rsz += int(rsz1); vsz += int(vsz1);
						fd += this.countOpenFD(apid);
					# else not adding memory usage
				line = JSTATUS.readline();
			JSTATUS.close();
			this.JOBS[pid]['DATA']['run_time'] = etime;
			this.JOBS[pid]['DATA']['cpu_time'] = cputime;
			this.JOBS[pid]['DATA']['cpu_usage'] = pcpu;
			this.JOBS[pid]['DATA']['mem_usage'] = pmem;
			this.JOBS[pid]['DATA']['rss'] = rsz;
			this.JOBS[pid]['DATA']['virtualmem'] = vsz;
			this.JOBS[pid]['DATA']['open_files'] = fd;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps --no-headers -eo \"pid ppid\"");
	
	# count the number of open files for the given pid
	def countOpenFD (this, pid):
		dir = '/proc/'+str(pid)+'/fd';
		if os.access(dir, os.F_OK):
			if os.access(dir, os.X_OK):
				list = os.listdir(dir);
				open_files = len(list);
				if pid == os.getpid():
					open_files -= 2;
				this.logger.log(Logger.DEBUG, "Counting open_files for "+ `pid` +": "+ str(len(list)) +" => " + `open_files` + " open_files");
				return open_files;
			else:
				this.logger.log(Logger.ERROR, "ProcInfo: cannot count the number of opened files for job "+`pid`);
		else:
			this.logger.log(Logger.ERROR, "ProcInfo: job "+`pid`+" dosen't exist");
	
	
	# if there is an work directory defined, then compute the used space in that directory
	# and the free disk space on the partition to which that directory belongs
	# sizes are given in MB
	def readJobDiskUsage (this, pid):
		if (pid == '') or not this.JOBS.has_key(pid):
			return;
		workDir = this.JOBS[pid]['WORKDIR'];
		if workDir == '':
			return;
		try:
			DU = os.popen("du -Lsck " + workDir + " | tail -1 | cut -f 1");
			line = DU.readline();
			this.JOBS[pid]['DATA']['workdir_size'] = int(line) / 1024.0;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run du to get job's disk usage for job "+`pid`);
		try:
			DF = os.popen("df -k "+workDir+" | tail -1");
			line = DF.readline().strip();
			m = re.match("\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%", line);
			if m != None:
				this.JOBS[pid]['DATA']['disk_total'] = float(m.group(1)) / 1024.0;
				this.JOBS[pid]['DATA']['disk_used']  = float(m.group(2)) / 1024.0;
				this.JOBS[pid]['DATA']['disk_free']  = float(m.group(3)) / 1024.0;
				this.JOBS[pid]['DATA']['disk_usage'] = float(m.group(4)) / 1024.0;
			DF.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run df to get job's disk usage for job "+`pid`);

	# create cummulative parameters based on raw params like cpu_, pages_, swap_, or ethX_
	def computeCummulativeParams(this, dataRef, prevDataRef):
		if prevDataRef == {}:
			for key in dataRef.keys():
				if key.find('raw_') == 0:
					prevDataRef[key] = dataRef[key];
			prevDataRef['TIME'] = dataRef['TIME'];
			return;
		
		# cpu -related params
		if (dataRef.has_key('raw_cpu_usr')) and (prevDataRef.has_key('raw_cpu_usr')):
			diff={};
			cpu_sum = 0;
			for param in ['cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle']:
				diff[param] = this.diffWithOverflowCheck(dataRef['raw_'+param], prevDataRef['raw_'+param]);
				cpu_sum += diff[param];
			for param in ['cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle']:
				if cpu_sum != 0:
					dataRef[param] = 100.0 * diff[param] / cpu_sum;
				else:
					del dataRef[param];
			if cpu_sum != 0:
				dataRef['cpu_usage'] = 100.0 * (cpu_sum - diff['cpu_idle']) / cpu_sum;
			else:
				del dataRef['cpu_usage'];
		
		# swap & pages -related params
		if (dataRef.has_key('raw_pages_in')) and (prevDataRef.has_key('raw_pages_in')):
			interval = dataRef['TIME'] - prevDataRef['TIME'];
			for param in ['pages_in', 'pages_out', 'swap_in', 'swap_out']:
				diff = this.diffWithOverflowCheck(dataRef['raw_'+param], prevDataRef['raw_'+param]);
				if interval != 0:
					dataRef[param] = 1000.0 * diff / interval;
				else:
					del dataRef[param];
		
		# eth - related params
		interval = dataRef['TIME'] - prevDataRef['TIME'];
		for rawParam in dataRef.keys():
			if (rawParam.find('raw_eth') == 0) and prevDataRef.has_key(rawParam):
				param = rawParam.split('raw_')[1];
				if interval != 0:
					dataRef[param] = this.diffWithOverflowCheck(dataRef[rawParam], prevDataRef[rawParam]); # absolute difference
					if param.find('_errs') == -1:
						dataRef[param] = dataRef[param] / interval / 1024.0; # if it's _in or _out, compute in KB/sec
				else:
					del dataRef[param];
		
		# copy contents of the current data values to the 
		for param in dataRef.keys():
			if param.find('raw_') == 0:
				prevDataRef[param] = dataRef[param];
		prevDataRef['TIME'] = dataRef['TIME'];
	

	# Return a hash containing (param,value) pairs with existing values from the requested ones
	def getFilteredData (this, dataHash, paramsList, prevDataHash = None):
	
		if not prevDataHash is None:
			this.computeCummulativeParams(dataHash, prevDataHash);
			
		result = {};
		for param in paramsList:
			if param == 'net_sockets':
			    for key in dataHash.keys():
			        if key.find('sockets') == 0 and key.find('sockets_tcp_') == -1:
				    result[key] = dataHash[key];
			elif param == 'net_tcp_details':
			    for key in dataHash.keys():
				if key.find('sockets_tcp_') == 0:
				    result[key] = dataHash[key];
			
			m = re.match("^net_(.*)$", param);
			if m == None:
				m = re.match("^(ip)$", param);
			if m != None:
				net_param = m.group(1);
				#this.logger.log(Logger.DEBUG, "Querying param "+net_param);
				for key, value in dataHash.items():
					m = re.match("eth\d_"+net_param, key);
					if m != None:
						result[key] = value;
			else:
				if param == 'processes':
					for key in dataHash.keys():
						if key.find('processes') == 0:
							result[key] = dataHash[key];
				elif dataHash.has_key(param):
					result[param] = dataHash[param];
		sorted_result = [];
		keys = result.keys();
		keys.sort();
		for key in keys:
			sorted_result.append((key,result[key]));
		return sorted_result;

######################################################################################
# self test

if __name__ == '__main__':
	logger = Logger.Logger(Logger.DEBUG);
	pi = ProcInfo(logger);
	
	print "first update";
	pi.update();
	print "Sleeping to accumulate";
	time.sleep(1);
	pi.update();
	
	print "System Monitoring:";
	sys_cpu_params = ['cpu_usr', 'cpu_sys', 'cpu_idle', 'cpu_nice', 'cpu_usage'];
	sys_2_4_params = ['pages_in', 'pages_out', 'swap_in', 'swap_out'];
	sys_mem_params = ['mem_used', 'mem_free', 'total_mem', 'mem_usage'];
	sys_swap_params = ['swap_used', 'swap_free', 'total_swap', 'swap_usage'];
	sys_load_params = ['load1', 'load5', 'load15', 'processes', 'uptime'];
	sys_gen_params = ['hostname', 'cpu_MHz', 'no_CPUs', 'cpu_vendor_id', 'cpu_family', 'cpu_model', 'cpu_model_name', 'bogomips'];
	sys_net_params = ['net_in', 'net_out', 'net_errs', 'ip'];
	sys_net_stat = ['sockets_tcp', 'sockets_udp', 'sockets_unix', 'sockets_icm'];
	sys_tcp_details = ['sockets_tcp_ESTABLISHED', 'sockets_tcp_SYN_SENT', 'sockets_tcp_SYN_RECV', 'sockets_tcp_FIN_WAIT1', 'sockets_tcp_FIN_WAIT2', 'sockets_tcp_TIME_WAIT', 'sockets_tcp_CLOSED', 'sockets_tcp_CLOSE_WAIT', 'sockets_tcp_LAST_ACK', 'sockets_tcp_LISTEN', 'sockets_tcp_CLOSING', 'sockets_tcp_UNKNOWN'];
	
	print "sys_cpu_params", pi.getSystemData(sys_cpu_params);
	print "sys_2_4_params", pi.getSystemData(sys_2_4_params);
	print "sys_mem_params", pi.getSystemData(sys_mem_params);
	print "sys_swap_params", pi.getSystemData(sys_swap_params);
	print "sys_load_params", pi.getSystemData(sys_load_params);
	print "sys_gen_params", pi.getSystemData(sys_gen_params);
	print "sys_net_params", pi.getSystemData(sys_net_params);
	print "sys_net_stat", pi.getSystemData(sys_net_stat);
	print "sys_tcp_details", pi.getSystemData(sys_tcp_details);
	
	job_pid = os.getpid();
	
	print "Job (mysefl) monitoring:";
	pi.addJobToMonitor(job_pid, os.getcwd());
	print "Sleep another second";
	time.sleep(1);
	pi.update();
	
	job_cpu_params = ['run_time', 'cpu_time', 'cpu_usage'];
	job_mem_params = ['mem_usage', 'rss', 'virtualmem', 'open_files'];
	job_disk_params = ['workdir_size', 'disk_used', 'disk_free', 'disk_total', 'disk_usage'];
	time.sleep(10);
	print "job_cpu_params", pi.getJobData(job_pid, job_cpu_params);
	print "job_mem_params", pi.getJobData(job_pid, job_mem_params);
	print "job_disk_params", pi.getJobData(job_pid, job_disk_params);
	
	pi.removeJobToMonitor(os.getpid());
