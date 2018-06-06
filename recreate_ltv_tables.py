#!/usr/bin/env python
from __future__ import print_function
import re
import sys
import time
import yaml
import json
from datetime import date
from pprint import pprint
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

def onboard_ltv_service():
    _, service_list = fdp_http_api.adminListServiceOnboarding(None, None, None, None, None, None)
    service_found = False
    for service in service_list:
        if service['service_name'] == service_name:
            service_found = True
    if not service_found:
        code1, request_id = fdp_http_api.adminRequestServiceOnboarding(service_name, [], 'the LTV service', 'test@uber.com')
        code2 = 400
        if code1 == 200:
            code2, result_text = fdp_http_api.adminApproveServiceOnboarding(request_id, [], 'test@uber.com')
        if code1 == 200 and code2 == 200:
            print("Successfully onboarded LTV service, this only needs to be done once")
        else:
            print("Failed in onboarding LTV service", code1, code2)
    else:
        print("LTV service has already onboarded")

'''
    LTV driver query is defined in ltv_driver_query.presto
    LTV rider query is defined in ltv_rider_query.presto
    LTV driver SQL table creation is defined in ltv_driver_table.sql
    LTV rider SQL table creation is defined in ltv_rider_table.sql
'''
ltv_driver_query_string = ''
ltv_rider_query_string = ''
ltv_driver_table_create = ''
ltv_rider_table_create = ''
def load_string_resources():
    global ltv_driver_query_string
    global ltv_rider_query_string
    global ltv_driver_table_create
    global ltv_rider_table_create
    with open('ltv_driver_query.presto', 'r') as myfile:
        ltv_driver_query_string=myfile.read()
    with open('ltv_rider_query.presto', 'r') as myfile:
        ltv_rider_query_string=myfile.read()
    with open('ltv_driver_table_create.sql', 'r') as myfile:
        ltv_driver_table_create=myfile.read()
    with open('ltv_rider_table_create.sql', 'r') as myfile:
        ltv_rider_table_create=myfile.read()

def recreate_curated_table(table_name, query_statement, create_table_statement):
    print("To delete prior table definition for %s..." % table_name)
    print(fdp_http_api.adminDeleteTableDefinition(None, service_name, table_name))
    print(fdp_http_api.adminCreateTableDefinition(
                service_name,
                table_name,
                table_name,
                ["xiaohai@uber.com"],
                create_table_statement,
                for_visualization=False, for_sharing=False,
                full_query_statement={"query_type": "PRESTO", "data_query": query_statement, "sql_data_table": table_name},
                incremental_query_statement=None,
                maintenance_query_statement=''
                ));
    print(fdp_http_api.adminPopulateDataForTableDefinition(None, service_name, table_name, None))

# Tunnel into staging/production environment to access FDP API
prepare_environment()

# you don't need to run service onboarding. Only need to run once, already done for staging
onboard_ltv_service()

# Read the text file contents
load_string_resources()

# The following two invocations create the SQL table and fill the contents. 
# They should be run only when there are changes to table structure
# remember to modify file contents at: 
#   ltv_driver_query.presto, ltv_driver_table_create.sql
#   ltv_rider_query.presto, ltv_rider_table_create.sql
#
# Queries are currently too slow, the API will timeeout, you can ignore it. 
# Data will be populated
#recreate_curated_table("ltv_driver", ltv_driver_query_string, ltv_driver_table_create)
#recreate_curated_table("ltv_rider", ltv_rider_query_string, ltv_rider_table_create)

