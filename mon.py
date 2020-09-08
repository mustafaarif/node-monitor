#!/cray_home/tamuq/env/bin/python3

# Author: Mustafa Arif
# Author Email: mustafa.arif@qatar.tamu.edu
# This script launches Node Level Monitoring scripts on raad2
# hosts which are running jobs from users under study.
# Once those running jobs are completed, Node level monitoring 
# is also terminated and log files are copied to a shared location. 

import os
from submods import *

logger = setup_logger('raad2 Node Monitor')
logger.info('raad2 Node Monitor Initalizing')

# Main function calls other methods and maintains code flow
def main():
  params = get_config()         		# Read main.cfg and extract config information.
  uList = params["uList"]			# Users under study list (defined in main.cfg)
  dbInfo = params["dbInfo"]			# Database connection information.
  
  try:
    jList = get_slurm_jobs(uList)               # Get running jobs list for users under study.
    if jList == 1:
      raise
  except:
    logger.error("Unable to talk to SLURM Controller. Terminating.")
  else:
    update_db(jList,dbInfo)			# Update state file with latest information we have in jList.
    logger.info("Stopping node monitor on nodes where job has already finished")
    stop_nodes_monitor(dbInfo)			# Stops node monitoring where job has finished.
    logger.info("Launching node monitor on nodes where still pending")
    start_nodes_monitor(dbInfo)			# Launch node monitoring where pending. 
    logger.info("Retrieving monitoring logs from nodes")
    retrieve_monitor_data(dbInfo)		# Retrieve logs from nodes
    logger.info("Program terminating..")

if __name__ == '__main__':
    main()
