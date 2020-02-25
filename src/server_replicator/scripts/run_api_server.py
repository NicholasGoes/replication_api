from flask import Flask, request

import src.server_replicator.core.BulkFunctionality as bf
from src.server_replicator.core.BulkFunctionality import executeQuery, executeBulkInsert

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Replication API developed by Nicholas Góes'

@app.route('/executeQuery', methods=['POST'])
def InvokeExecuteQuery():
    if(request.is_json):
        content = request.get_json()
        execute_query = content['execute_query']
        target_server = content['target_server']

        return executeQuery(execute_query, target_server)
    else:
        return 'Parameter is not JSON'

@app.route('/executeBulkInsert', methods=['POST'])
def InvokeExecuteBulkInsert():
    if(request.is_json):
        content = request.get_json()
        origin_query = content['origin_query']
        origin_table = content['origin_table']
        target_table = content['target_table']
        database_name = content['database_name']
        origin_server = content['origin_server']
        target_server = content['target_server']
        truncate_table = content['truncate_table']
        
        return executeBulkInsert(origin_table, target_table, database_name, origin_server, target_server, origin_query, truncate_table)
    else:
        return 'Parameter is not JSON'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

##@app.route('/<string:database_name>/<string:target_table>', methods=['GET'])
##def invokeFullBulkInsert(database_name, target_table):
##    print(target_table)
##    ret = bf.fullBulkInsert(target_table, target_table, database_name, '190.1.1.2', '172.20.1.45')
##    return ret
