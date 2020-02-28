import subprocess
from typing import Optional
from sys import platform

_CREDENTIALS = r'''
    $SecurePassword = ConvertTo-SecureString "pyBBsd4C0ZhuciXQzNQZ" -AsPlainText -Force
    $cred = New-Object System.Management.Automation.PSCredential ("qb_replication", $SecurePassword)
    '''
_VAR_CMDLET = _CREDENTIALS + r'''
    $origin_table = "%origin_table%"
    $target_table = "%target_table%"
    $database_name = "%database_name%"
    $origin_server = "%origin_server%"
    $target_server = "%target_server%"
    $origin_query = "%origin_query%"
    $execute_query = "%execute_query%"
    '''

def executeBulkInsert(origin_table: str,
                      target_table: str,
                      database_name: str,
                      origin_server: str,
                      target_server: str,
                      origin_query: Optional[str] = None,
                      truncate: Optional[str] = 'false'):
    """Execute bulk insert using the parameters. If `origin_query` was not informed, full bulk insert on table will be executed.
    Examples:
            >>> result = executeBulkInsert('tab_pedido', 'tab_pedido', 'dbcomfri', '190.1.1.2', '172.20.1.45')
            >>> result_query = executeBulkInsert('tab_pedido', 'tab_pedido', 'dbcomfri', '190.1.1.2', '172.20.1.45', 'SELECT * FROM tab_pedido', 'true')
    Args:
        origin_table: Origin Table
        target_table: Target Table
        database_name: Database Name
        origin_server: Origin Server
        target_server: Target Server
        origin_query: Query used to make retreive data to bulk insert on target
        truncate: Truncate table before bulk insert ('true' or 'false')
    Returns:
        String containing the execution result        
    """
    cmdlet = _VAR_CMDLET
    cmdlet = cmdlet.replace('%origin_table%', origin_table)
    cmdlet = cmdlet.replace('%target_table%', target_table)
    cmdlet = cmdlet.replace('%database_name%', database_name)
    cmdlet = cmdlet.replace('%origin_server%', origin_server)
    cmdlet = cmdlet.replace('%target_server%', target_server)
    
    if truncate == 'true':
        truncate_opt = ' -Truncate'
    else:
        truncate_opt = ''

    if origin_query is None or origin_query == '':        
        executable_cmdlet = cmdlet + r'''
        Copy-DbaDbTableData -SqlCredential $cred -SqlInstance $origin_server -Destination $target_server -Database $database_name -Table $origin_table -DestinationDataBase $database_name -DestinationTable $target_table -bulkCopyTimeOut 36000 -DestinationSqlCredential $cred -KeepNulls -BatchSize 100000''' +truncate_opt+ '''
        '''
    else:
        cmdlet = cmdlet.replace('%origin_query%', origin_query)
        executable_cmdlet = cmdlet + r'''
        Copy-DbaDbTableData -Query $origin_query -SqlCredential $cred -SqlInstance $origin_server -Destination $target_server -Database $database_name -Table $origin_table -DestinationDataBase $database_name -DestinationTable $target_table -bulkCopyTimeOut 36000 -DestinationSqlCredential $cred -BatchSize 100000''' +truncate_opt+ '''
        '''
    print(f'''Executed bulkInsert - {target_table}\n''')
    return _execCmdlet(executable_cmdlet)

def executeQuery(execute_query,
                 target_server):
    """Execute any SQL command using the parameters.
    Examples:
            >>> result_query = executeQuery('DELETE tp FROM tab_pedido tp WHERE num_pedido = 123456', '172.20.1.45')
    Args:
        execute_query: SQL Command to be executed
        target_server: Target Server
    Returns:
        String containing the execution result        
    """
    cmdlet = _VAR_CMDLET
    cmdlet = cmdlet.replace('%execute_query%', execute_query)
    cmdlet = cmdlet.replace('%target_server%', target_server)

    executable_cmdlet = cmdlet + r'''
    Invoke-DbaQuery -Query $execute_query -SqlCredential $cred -SqlInstance $target_server -QueryTimeout 36000'''
    print(f'''Executed executeQuery - {execute_query}\n''')
    #print(executable_cmdlet)
    return _execCmdlet(executable_cmdlet)

def _execCmdlet(executable_cmdlet: str):
    print(executable_cmdlet)
    if platform == 'win32':
        p = subprocess.Popen(['powershell', executable_cmdlet], stdout=subprocess.PIPE).communicate()[0]
    else:
        p = subprocess.Popen(['pwsh', "-Command", executable_cmdlet], stdout=subprocess.PIPE).communicate()[0]
    return p.decode('utf-8')     

import time

# if __name__ == '__main__':
#     t = time.time()
#     print(executeBulkInsert("[dbo].[tab_valor_mercado_carne_osso]", "[dbo].[tab_valor_mercado_carne_osso]", 'dbsupfri', '190.1.1.2', '172.20.1.45', truncate='true'))
#     print(time.time()-t)
##    deleteCurrentSnapshotDate('tab_choice_modo_conservacao_snapshot', 'dbsupfri', '172.20.1.45')    
##    fullBulkInsert('tab_choice_modo_conservacao', 'tab_choice_modo_conservacao_snapshot', 'dbsupfri', '190.1.1.2', '172.20.1.45')
