#!/usr/bin/env python
from __future__ import print_function
import subprocess
import socket
from contextlib import closing
from lib.fdp_python_http_api import FDPHttpApi

'''
    To create the FDP API object

      Please change the user name to your own ldap name
      
      FDP staging environment's port number is 14361, 
      and the production environment's port is 19026.
      This script assumes staging port. 
      Please change as necessary

'''
service_name = "ltv"
operation_port=14361
fdp_http_api = FDPHttpApi(user_name='xiaohai', port=operation_port)

def prepare_environment():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex(("127.0.0.1", operation_port)) != 0:
            print("To establish tunnling ...")
            subprocess.call(["ssh -f adhoc10-sjc1 -L %d:localhost:%d -N" % (operation_port, operation_port)], shell=True)
    print("To check the service health");
    subprocess.call(["curl http://localhost:%d/health" % operation_port], shell=True)
    print("")

prepare_environment()

# The following demonstrates how to query the FDP SQL table
status_code, results_string = fdp_http_api.queryData(service_name, None, "select * from ltv_driver where city_id=328", None, "CSV")
print('query results:')
print(results_string)
