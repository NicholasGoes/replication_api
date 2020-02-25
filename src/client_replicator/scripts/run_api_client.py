import time
import csv
import colorama

from multiprocessing.pool import ThreadPool
from threading import Thread,Event
from prettytable import PrettyTable
from clear_screen import clear

import sys
#sys.path.append("..")

import src.client_replicator.utils.convertShelltoHTML as sh

from src.client_replicator.core.API_Functions import API_Functions
from src.client_replicator.utils.perpetualTimer import perpetualTimer


_API_ENDPOINT = "http://172.20.1.45:5000"

execution_results = {}
strftime = ''

def executeMainProcess(args): 
    database_name = args[0]
    schema_name = args[1]
    table_name = args[2]
    origin_server = args[3]
    target_server = args[4] 
    origin_query = args[5]
    delete_query = args[6]

    api = API_Functions(api_endpoint=_API_ENDPOINT)

    if len(origin_query)==0:       
        api.executeBulkInsert(execution_results, table_name, table_name, database_name, origin_server, target_server)
    else:
        truncate_table = 'true'
        if len(delete_query)!=0:
           api.executeQuery(execution_results, table_name, delete_query, target_server)
           truncate_table = 'false'
        api.executeBulkInsert(execution_results, table_name, table_name, database_name, origin_server, target_server, origin_query, truncate_table)
    return 1

def executeLogProcess():
    tb = PrettyTable(field_names = ["Table Name", "Status"])
    snapshot_executions = execution_results.items()
    for key,val in snapshot_executions:
        tb.add_row([key, val])
    clear()
    print(tb)
    exportLog()
    

def exportLog():
    tb = PrettyTable(field_names = ["Table Name", "Status"])
    snapshot_executions = execution_results.items()
    for key,val in snapshot_executions:
        tb.add_row([key, val])
    html_result = tb.get_html_string()
    with open(f'log/replication_log_{strftime}.html','w') as f:
        f.write(sh.convertShellToHTML(html_result))
        
if __name__ == '__main__':
    colorama.init()
    with open('conf/replication_list_git.csv') as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')

        args = [[row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in csv_reader]
        args.pop(0)

        t=time.time()
        strftime = time.strftime("%Y%m%d_%H-%M")

        x = perpetualTimer(10, executeLogProcess)
        x.start()
        
        with ThreadPool(4) as pool:
            results = pool.map(executeMainProcess, args)

        x.cancel()
        executeLogProcess()
        exportLog()

        print(time.time() - t)
        print(results)
