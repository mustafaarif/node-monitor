#!/cray_home/tamuq/env/bin/python3

# Author: Mustafa Arif
# Email: mustafa.arif@qatar.tamu.edu
import os
import logging
import mysql.connector 
import csv
import configparser
import subprocess
import time


logger = logging.getLogger('raad2 Node Monitor')

# Read configuration file and extract required parameters
def get_config():
  logger.info("Reading configuration file.")
  config = config = configparser.ConfigParser()
  config.read('main.cfg')
  uList = config['users']['uList']         # A comma separated list of users under study.
  dbInfo = dict(config.items('database'))  # Dict item containing DB info.
  params = {'uList':uList,'dbInfo':dbInfo}
  return(params)

# get_slurm_jobs() fetches running jobs information using slurm command.
# Job list is filtered with user(s) information passed as argument to this function.
# Filtered list is then returned.
def get_slurm_jobs(uList):
  logger.info("Fetching SLURM Jobs..")
  sCmd = "sudo /opt/slurm/17.11.5/bin/squeue -u " + uList + " -t RUNNING -p l_long,adm_debug --noheader --format='%A %.9u %T %N'"
  p = subprocess.Popen(sCmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  p.wait()
  stdout, stderr = p.communicate()
  if str(stdout).find("ERROR") == -1:
    stdout = stdout.decode('ascii')
    tList = stdout.split('\n')
    tList = list(filter(None,tList))
    jList = []
    for index in range(0,len(tList)):
      jList.append(tList[index].split())
    jList = fix_nid_notation(jList)				# Fix nid notations
    jList = rm_nonexclusive_jobs(jList)				# Remove jobs where job is sharing a node
    [n.extend(['PENDING','ProcessList=','NULL','NULL','NULL','NULL','NULL']) for n in jList]	 # Add columns for monitoring for node mon status, pname and dates
    return(jList)
  else:
    return(1)

# update_db() receives joblist and database connection info.
# Writes/Updates Job information in DB
def update_db(jList,dbInfo):
  
  # Open DB Connection 
  logger.info("Updating database with new jobs list fetched from slurm..")
  logger.info("Opening connection to database %s at host %s with username %s", dbInfo['db'], dbInfo['host'],dbInfo['user'])
  cnx = mysql.connector.connect(host=dbInfo['host'], user=dbInfo['user'], password=dbInfo['password'], database=dbInfo['db'])
  cursor = cnx.cursor()

  # Write new records to database
  for job in jList:
    data = { 'jid': job[0],'user': job[1], 'jobstate': job[2],'nids': ','.join(job[3]),'monstatus': job[4],'pnames': job[5],'jobstart': job[6],'monstart': job[7],'jobend': job[8],'monend': job[9],'retrievelogs': job[10], }
    write_to_db(data,cnx)
  
  # For jobs which are no longer in slurm, update their records in DB and mark them FINISHED
  sqlQuery = ("select jid,monstatus from master where jobstate = 'RUNNING'") 
  cursor.execute(sqlQuery)
  dbList = cursor.fetchall()
  dbList = [str(item[0]) for item in dbList]	# List of jids extracted from db
  jidList = [str(item[0]) for item in jList]	# List of jids extracted from jlist (slurm)
  for jid in dbList:				#dbList = Records in DB where jobstate = RUNNING
    if jid not in jidList:			#jidList = Obtained from current state of SLURM
      # For this job update jobstate to FINISHED
      # Logic: Extract records from DB where jobstate=running. For each record in DB, check if 
      #         this records exists in data obtained from slurm. If not, this means job has already finished.
      #         Update record in DB by marking this job as finished.
      sqlQuery = "update master set jobstate='FINISHED' where jid=%(jid)s"
      cursor.execute(sqlQuery,{'jid':jid})   
      cnx.commit()
  cursor.close()
  cnx.close()

# Launch monitoring on nodes where still pending
# This function opens a connection to DB and fetches record where Job is running but mon is still OFF
# These records (jid and nidlist) is passed to a function mon_launcher(jid,nidList)
# mon_launcher() launches monitoring on these nodes and upon success returns 0
# monstatus is then changed to ON upon success return from previous.
def start_nodes_monitor(dbInfo):
  cnx = mysql.connector.connect(host=dbInfo['host'], user=dbInfo['user'], password=dbInfo['password'], database=dbInfo['db'])
  sqlQuery = ("select jid, nids, user, annonjid from master where monstatus = 'PENDING' AND jobstate = 'RUNNING'")
  cursor = cnx.cursor()
  cursor.execute(sqlQuery)
  jList = cursor.fetchall()
  for job in jList:
    nidList = job[1].split(',')
    mon_launcher(job[0],nidList,job[2],job[3])		# Send annonid and nidlist to mon_launcher
    tObj = time.strftime('%Y-%m-%d %H:%M:%S')
    sqlQuery = "update master set monstatus='RUNNING', monstart= %(tObj)s, retrievelogs='PENDING' where jid=%(jid)s"
    cursor.execute(sqlQuery,{'tObj':tObj,'jid':job[0]})
    cnx.commit()
    pNames = pnames_fetcher(nidList,job[2])						# Extract process names from host running Job
    sqlQuery = "update master set pnames=CONCAT(pnames,%(pNames)s) where jid=%(jid)s"
    cursor.execute(sqlQuery,{'pNames':pNames,'jid':job[0]})
    cnx.commit()
  cursor.close()
  cnx.close()

# Reads state file and stop moniotring on nodes where job has already finished
def stop_nodes_monitor(dbInfo):
  cnx = mysql.connector.connect(host=dbInfo['host'], user=dbInfo['user'], password=dbInfo['password'], database=dbInfo['db'])
  sqlQuery = ("select jid, nids from master where monstatus = 'RUNNING' AND jobstate = 'FINISHED'")
  cursor = cnx.cursor()
  cursor.execute(sqlQuery)
  jList = cursor.fetchall()
  for job in jList:
    nidList = job[1].split(',')
    mon_stopper(job[0],nidList)
    tObj = time.strftime('%Y-%m-%d %H:%M:%S')
    sqlQuery = "update master set monstatus='COMPLETED', monend= %(tObj)s where jid=%(jid)s"
    cursor.execute(sqlQuery,{'tObj':tObj,'jid':job[0]})
    cnx.commit()
  cursor.close()
  cnx.close()

def retrieve_monitor_data(dbInfo):
  cnx = mysql.connector.connect(host=dbInfo['host'], user=dbInfo['user'], password=dbInfo['password'], database=dbInfo['db'])
  sqlQuery = ("select jid, nids,annonjid from master where monstatus = 'COMPLETED' AND retrievelogs = 'PENDING'")
  cursor = cnx.cursor()
  cursor.execute(sqlQuery)
  jList = cursor.fetchall()
  for job in jList:
    nidList = job[1].split(',')
    logs_fetcher(nidList,job[2])
    sqlQuery = "update master set retrievelogs='COMPLETED' where jid=%(jid)s"
    cursor.execute(sqlQuery,{'jid':job[0]})
    cnx.commit()
  cursor.close()
  cnx.close()

#############################################################################
#                         SUPPLEMENTARY FUNCTIONS                           #
#############################################################################

def pnames_fetcher(nidList,user):
  all_pNames = ""
  for HOST in nidList: 
    COMMAND = 'ps -u %s | awk \'{ print $4 }\'' % (user)
    pNames = subprocess.check_output(["ssh", "%s" % HOST, COMMAND])
    pNames = str(pNames)
    pNames = pNames.replace('b\'','')
    pNames = pNames.replace('CMD\\n','')
    pNames = pNames.replace('srun\\n','')
    pNames = pNames.replace('\\nslurm_script\\n','')
    pNames = pNames.replace('\\n',',')
    all_pNames = all_pNames+HOST+":{"+pNames+"};"
  return(all_pNames)

def logs_fetcher(nidList,annonjid):
  for HOST in nidList:
    fPath="/tmp/pfmon-"+str(annonjid)+"-"+HOST+".dat"
    cmd = "rsync -avz --remove-source-files -e ssh "+HOST+":"+fPath+" /tmp/raad2_mon_perf_logs"
    logger.debug("Fetching node monitoring logs from %s with command %s", HOST ,cmd)
    os.system(cmd)

def mon_launcher(jid,nidList,user,annonjid):
  for HOST in nidList:
    logger.debug("Launching node monitoring for Jid: %s on nid %s", jid, HOST)
    # Copy Node Monitoring script to node
    cmd = "scp "+os.getcwd()+"/node_monitor.py root@"+HOST+":/tmp/"
    logger.debug("Copying Node Monitoring script to Host %s with command %s", HOST ,cmd)
    os.system(cmd)
    # Change permission of script
    COMMAND = "chmod +x /tmp/node_monitor.py"
    p = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p.wait()
    # Launch Node Monitor script on remote HOST
    COMMAND = "/tmp/node_monitor.py -j "+str(annonjid)+" -i 120 -u "+user+" >&- &"
    logger.debug("Launching Node Monitor on %s over ssh using command %s",HOST,COMMAND)
    p = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p.wait()
  return(0)

def mon_stopper(jid,nidList):
  for HOST in nidList:
    COMMAND = "ps -ef | grep node_monitor.py | awk '{print $2}' | xargs kill -9"
    logger.debug("Stopping node monitoring for Jid: %s on nid %s using command %s", jid, HOST,COMMAND)
    p = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p.wait()
    COMMAND = "rm /tmp/node_monitor.py"
    logger.debug("Removing Node Monitoring script from Host %s with command %s", HOST, COMMAND)
    p = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p.wait()
  return(0) 

def write_to_db(data,cnx):
  cursor = cnx.cursor()
  sqlStatement = "INSERT IGNORE INTO master (jid, user, jobstate, nids, monstatus, pnames, jobstart, monstart, jobend, monend) VALUES ( %(jid)s, %(user)s, %(jobstate)s, %(nids)s, %(monstatus)s, %(pnames)s, %(jobstart)s, %(monstart)s, %(jobend)s, %(monend)s)"
  cursor.execute(sqlStatement,data)
  cnx.commit()
  cursor.close()

# nid_notation() receives jobs list and fix nid notations
# nid00[040,041] will be converted to nid00040,nid00041
def fix_nid_notation(jList):
  tmp = []
  for job in range(0,len(jList)):                               # For each job in jList
      nids = jList[job][3]                                      # Get Nids list
      if nids.find('[') != -1:                                  # If nid is of notation nid00[040,041] etc.
        nids = nids.split('[')[1].split(']')[0].split(',')      # Split the nids list to get a list containing ['040','041']
        tmp = nids.copy()
        for nid in tmp:                                         # For each nid check if range is noted with a notation nid00[392-393]
          if nid.find('-') != -1:
            tnids = nid.split('-')                              # Split on '-'
            nids.extend(list(range(int(tnids[0]),int(tnids[1]) + 1))) # From first node till last node make a list of all integers and extend the tmp list
            nids.remove(nid)                                    # Remove that nid from the original list
        nids = [str(n).zfill(5) for n in nids]                  # Make length of nid name to 5 by appending missing ZEROS
        nids = ['nid' + n for n in nids]                        # Append string 'nid' with nid index
        jList[job][3] = nids.copy()                             # Copy new nid list to jList
      elif nids.find('[') == -1:                                # If its just a single nid representation, convert it to list.
        jList[job][3]=jList[job][3].split()
  return(jList)



# Create logger function
def setup_logger(name):
  logger = logging.getLogger(name)				# Create logger with name
  logger.setLevel(logging.DEBUG)
  fh = logging.FileHandler('monitor.log')			# Create file handler which logs debug messages
  fh.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()					# Console handler with a higher log level
  ch.setLevel(logging.ERROR)
  formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
  fh.setFormatter(formatter)
  ch.setFormatter(formatter)
  logger.addHandler(fh)						# Add handlers to logger
  logger.addHandler(ch)
  return logger


# rem_shared_jobs() removes jobs from jList where a job shares a node with other
def rm_nonexclusive_jobs(jList):
  nids = []
  dup_flag = 0
  for records in range(0,len(jList)):
    for nid in jList[records][3]:
      nids.extend([nid])
      if len(nids) != len(set(nids)):				# If found duplicates, make list of those nids
        dup_flag = 1
  if dup_flag == 1:
    logger.debug("Duplicate Nids found.." )
    dup_nids = []
    freq = {x:nids.count(x) for x in nids}
    dup_nids = [x for x in freq.keys() if freq[x]>1]
    prob_jobs = []
    for dup_nid in dup_nids:
      for records in range(0,len(jList)):
        for nid in jList[records][3]:
          if dup_nid == nid:
            prob_jobs.extend([jList[records][0]])
    for jid in prob_jobs:					# For each job id in problematic jobs, remove them from jList
      jList = [job for job in jList if job[0] != jid]
    logger.debug("Jobs which will not be monitored %s", prob_jobs)
  else:
    logger.debug("No duplicate nids found..")
  return(jList)

