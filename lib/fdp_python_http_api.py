from __future__ import print_function
import re
import sys
import time
import yaml
import json
from datetime import date

# pip install -r requirements.txt
import requests

class FDPHttpApi:

    def __init__(self, user_name='', host='localhost', port=8080):
        '''
        Description:
            to create the http api object
        parameters:
            user_name: your uber user name (ldap name)
            host: the host name
            port: the port number
        '''
        self._user_name = user_name
        self._fdp_url_base = 'http://%s:%d' % (host, port)

    def executeOnQB(self, query, params, is_permalink=False, sink='CSV'):
        '''
        Description:
            to execute QB queries with parameters
            Need to first tunnel to production servers at port 14456 from local 7033 in development
            e.g. ssh -f compute368-sjc1 -L 7033:localhost:14456 -N
            Also needs to tunnel into Presto because queries are carried out via Presto
            e.g. ssh -f compute368-sjc1 -L 7031:prestomaster05-sjc1:8080 -N
        Parameters:
            query: the query statement itself or the permalink id
            is_permalink: if the query parameter contains a permalink_id
            params: a dictionary of parameters for the query
            sink: needs to be 'JSON', 'KAFKA' or 'CSV'
        Returns:
            Text or JSON depending on request results
        '''
        url = self._fdp_url_base + '/qb/api/v1/qabc'
        query_data = dict(params)
        query_data['query'] = query
        query_data['sink'] = sink
        if is_permalink:
            query_data['is_permalink'] = 1
        query_data['X-Auth-Params-Email'] = self._user_name + "@uber.com"
        response = requests.get(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code != 200 or self.get_content_type(response) == "text/plain":
            return response.text
        else:
            return response.json()

    def get_content_type(self, response):
        '''
        Description:
            Some servers return content type header key as Content-Type while others return content-type.
            Consolidate the content type header processing here.
        Parameters:
            response: the HTTP response object returned from HTTP request objects
        Returns:
            the content type header string
        '''
        if 'Content-Type' in response.headers.keys():
            return response.headers['Content-Type']
        if 'content-type' in response.headers.keys():
            return response.headers['content-type']
        return "text/plain"

    def adminGetNotificationEmailList(self):
        '''
        Description:
            To return the administrative email list.
        Parameters:
            None
        Returns:
            the error code and error message in case of failure
            the success status code, and a list of email addresses in JSON
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/getNotificationEmailList'
        response = requests.get(url, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code != 200 or self.get_content_type(response) == "text/plain":
            return response.status_code, response.text
        else:
            return response.status_code, response.json()

    def adminSetNotificationEmailList(self, email_list):
        '''
        Description:
            To set the administrative email list.
        Parameters:
            email_list: the list of administrative email in JSON format
        Returns:
            the error code and error message in case of failure
            the success status code, and an empty text
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/setNotificationEmailList'
        query_data = {"email_list": email_list}
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code == 200:
            return response.status_code, ""
        else:
            return response.status_code, response.text

    def adminRequestServiceOnboarding(self, service_name, data_source_list, description, requester_email, to_withdraw=False):
        '''
        Description:
            To issue or withdraw service onboarding requests.
        Parameters:
            service_name: the name of the service being onboarded
            data_source_list: the list of tables to access
            description: any description text, expected to be business reasons for onboarding
            requester_email: the author of the request
            to_withdraw: to request onboarding or to cancel a prior request
        Returns:
            the status code, and any descriptive text in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/requestServiceOnboarding'
        query_data = {
            "service_name": service_name,
            "data_source_list": data_source_list,
            "description": description,
            "requester_email": requester_email,
            "to_withdraw": to_withdraw
        }
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminApproveServiceOnboarding(self, request_id, data_source_list, operator, to_reject=False):
        '''
        Description:
            To approve or reject a service onboarding request.
        Parameters:
            request_id: the id of the request for which to approve or reject
            data_source_list: the list of data tables for whose reading access is granted to the service
            operator: the operator email that is approving/rejecting
            to_reject: the flag it indicate approving or rejecting
        Returns:
            the status code, and error messages in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/approveServiceOnboarding'
        query_data = {
            "request_id": request_id,
            "data_source_list": data_source_list,
            "operator": operator,
            "to_reject": to_reject
        }
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminListServiceOnboarding(self, later_than, request_id, pending_only, requested_by, operator, service_name):
        '''
        Description:
            To list service onboarding requests by various query patterns. Leaving any of
            the parameters as None means no filtering on that field.
        Parameters:
            later_than: to indicate query starting data if specified
            request_id: to indicate a specific service onboarding request if specified
            pending_only: to return requests that are pending only if specified
            requested_by: to filter the service onboarding requests by the requester email address if specified
            operator: to filter the service onboarding requests by the operator email address if specified
            service_name: to filter the service onboarding requests by service name if specified
        Returns:
            the error code and error message in case of failure
            the success status code, and a list of email addresses in JSON
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/listServiceOnboarding'
        query_data = {
            "later_than": later_than,
            "request_id": request_id,
            "pending_only": pending_only,
            "operator": operator,
            "requested_by": requested_by,
            "service_name": service_name
        }
        response = requests.get(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code != 200 or self.get_content_type(response) == "text/plain":
            return response.status_code, response.text
        else:
            return response.status_code, response.json()

    def adminCreateTableDefinition(self, owning_service, table_name, table_description, owners,
            create_table_statement, for_visualization=False, for_sharing=False, full_query_statement={"query_type": "MYSQL", "data_query": ""},
            incremental_query_statement={"query_type": "MYSQL", "data_query": ""}, maintenance_query_statement=''
            ):
        '''
        Description:
            To create a new certified table definition
        Parameters:
            owning_service: the name of the service that owns the certified table definition
            table_name: the name of the certified table
            table_description: a descriptive text about the purpose of the table
            onwers: a list of email addresses that owns the table definition
            create_table_statement: the SQL DML statement that creates the data table
            for_visualization: a flag to indicate if the certified table definition is for visualization
            for_sharing: a flag to indicate if the certified table definition is for data sharing purpose
            full_query_statement: the query statement to initially fill the data table in JSON string format
            incremental_query_statement: the query statement to periodically refresh the data table in JSON string format
            maintenance_query_statement: the mySQL statement to periodically clean stale data from the data table
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/createTableDefinition'
        data = {
            "owning_service": owning_service,
            "table_name": table_name,
            "table_description": table_description,
            "owners": owners,
            "create_table_statement": create_table_statement,
            "for_visualization": for_visualization,
            "for_sharing": for_sharing,
            "full_query_statement": full_query_statement,
            "incremental_query_statement": incremental_query_statement,
            "maintenance_query_statement": maintenance_query_statement
        }
        response = requests.put(url, json=data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminListTableDefinitions(self, table_id, owning_service, table_name, for_visualization, for_sharing):
        '''
        Description:
            To return the certified table definition list.
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
            for_visualization: to filter the list by returning tables that are marked for visualization
            for_sharing: to filter the list by returning tables that are marked for sharing
        Returns:
            the error code and error message in case of failure
            the success status code, and a list of certified table definitions in JSON
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/listTableDefinitions'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name,
            "for_visualization": for_visualization,
            "for_sharing": for_sharing
        }
        response = requests.get(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code != 200 or self.get_content_type(response) == "text/plain":
            return response.status_code, response.text
        else:
            return response.status_code, response.json()

    def adminDeleteTableDefinition(self, table_id, owning_service, table_name):
        '''
        Description:
            To delete a certified table definition by id or name.
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/deleteTableDefinition'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name
        }
        response = requests.delete(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminScheduleTableDefinitionRun(self, table_id, owning_service, table_name,
            full_query_scheduling, incremental_query_scheduling, maintenance_query_scheduling):
        '''
        Description:
            To schedule a certified table run by id or name.
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
            full_query_scheduling: the scheduling for running the full query statement
            incremental_query_scheduling: the scheduling for running the incremental query statement
            maintenance_query_scheduling: the scheduling for running the maintenance query statement
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/scheduleTableDefinitionRun'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name,
            "full_query_scheduling": full_query_scheduling,
            "incremental_query_scheduling": incremental_query_scheduling,
            "maintenance_query_scheduling": maintenance_query_scheduling
        }
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminCancelTableDefinitionScheduling(self, table_id, owning_service, table_name,
            cancel_full_query_scheduling, cancel_incremental_query_scheduling, cancel_maintenance_query_scheduling):
        '''
        Description:
            To schedule a certified table run by id or name.
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
            cancel_full_query_scheduling: if to cancel the scheduling for running the full query statement
            cancel_incremental_query_scheduling: if to cancel the scheduling for running the incremental query statement
            cancel_maintenance_query_scheduling: if to cancel the scheduling for running the maintenance query statement
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/cancelTableDefinitionScheduling'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name,
            "cancel_full_query_scheduling": cancel_full_query_scheduling,
            "cancel_incremental_query_scheduling": cancel_incremental_query_scheduling,
            "cancel_maintenance_query_scheduling": cancel_maintenance_query_scheduling
        }
        response = requests.delete(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminCleanDataForTableDefinition(self, table_id, owning_service, table_name):
        '''
        Description:
            To clean the data table of certified table definition
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/cleanDataForTableDefinition'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name
        }
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminPopulateDataForTableDefinition(self, table_id, owning_service, table_name, query_params, no_table_truncating=None):
        '''
        Description:
            To manually run the full query statement to populate the data table of the certified table definition
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
            query_params: the query parameters in a dictionary
            no_table_truncating: a flag about whether or not to truncate the data table before the running the full query
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/populateDataForTableDefinition'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name,
            "no_table_truncating": no_table_truncating
        }
        response = requests.put(url, json=query_params, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminUpdateDataForTableDefinition(self, table_id, owning_service, table_name, query_params):
        '''
        Description:
            To manually run the incremental query statement to update the data table of the certified table definition
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
            query_params: the query parameters in a dictionary
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/updateDataForTableDefinition'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name
        }
        response = requests.put(url, json=query_params, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def adminRunMaintenanceForTableDefinition(self, table_id, owning_service, table_name):
        '''
        Description:
            To manually run the maintenance query statement to update the data table of the certified table definition
        Parameters:
            table_id: the id of the certified table definition if specifed
            owning_service: the owning service name for the query if specified
            table_name: the certified table name if specified
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/admin/runMaintenanceForTableDefinition'
        query_data = {
            "table_id": table_id,
            "owning_service": owning_service,
            "table_name": table_name
        }
        response = requests.put(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def queryData(self, owning_service, query_type, query_statement, query_parameters, response_format):
        '''
        Description:
            To query the data table of a certified table definition
        Parameters:
            owning_service: the owning service name for the query
            query_statement: a mySQL query statement
            query_parameters: parameters for the query statement in a dictionary
            response_format: CSV or JSON output format
        Returns:
            the error code and error message in case of failure
            the success status code, and result string in CSV or JSON format
        '''
        url = self._fdp_url_base + '/api/v1/sql/queryData'
        data = {
            "owning_service": owning_service,
            "query_type": query_type,
            "query_statement": query_statement,
            "query_parameters": query_parameters,
            "response_format": response_format
        }
        response = requests.post(url, json=data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        if response.status_code != 200 or self.get_content_type(response) == "text/plain":
            return response.status_code, response.text
        else:
            return response.status_code, response.json()

    def updateData(self, owning_service, sql_statements, sql_parameters, upload_table_name, upload_csv_content, is_append=None):
        '''
        Description:
            To update the data table of a certified table definition. Users can update the data
            table by using either the sql_statements or upload_csv_content
        Parameters:
            owning_service: the owning service name for the query
            sql_statements: a list of mySQL update/insert statements if specified
            sql_parameters: parameters for the query statement if any as a dictionary for convenience
            upload_table_name: the data table name
            uploaded_csv_content: the CSV contents to upload into the data table if specified
            is_append: flag to indicate if the CSV contents are appended to the data table
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/updateData'
        data = {
            "owning_service": owning_service,
            "sql_statements": sql_statements,
            "sql_parameters": sql_parameters,
            "upload_table_name": upload_table_name,
            "uploaded_csv_content": upload_csv_content,
            "is_append" : is_append
        }
        response = requests.post(url, json=data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"})
        return response.status_code, response.text

    def uploadCsv(self, owning_service, upload_table_name, csv_path, is_append=None):
        '''
        Description:
            To upload the content of a CSV file into a data table.
        Parameters:
            owning_service: the owning service name for the query
            upload_table_name: the data table name
            csv_path: the CSV file path
            is_append: flag to indicate if the CSV contents are appended to the data table
        Returns:
            the status code, and an error message in case of failure
        '''
        url = self._fdp_url_base + '/api/v1/sql/uploadCsv'
        query_data = {
            "owning_service": owning_service,
            "upload_table_name": upload_table_name,
            "is_append" : is_append
        }
        response = requests.post(url, params=query_data, headers={"X-Uber-Source": "python", "X-Auth-Params-Email": self._user_name + "@uber.com"}, files={'file': open(csv_path, 'r')})
        return response.status_code, response.text
