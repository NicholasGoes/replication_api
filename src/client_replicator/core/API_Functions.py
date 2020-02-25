import requests 
from time import time
from colorama import Fore, Back, Style
from typing import Dict, Optional

# defining the api-endpoint
#API_ENDPOINT = "http://172.20.1.45:5001/"
#API_ENDPOINT_QUERY = "http://172.20.1.45:5001/executeBulkInsert"
#API_ENDPOINT_DELETE = "http://172.20.1.45:5001/executeQuery"

class API_Functions:
    def __init__(self, api_endpoint: str):
        self._api_endpoint = api_endpoint
        self._api_endpoint_bulk = f'{self._api_endpoint}/executeBulkInsert'
        self._api_endpoint_delete = f'{self._api_endpoint}/executeQuery'

    def executeQuery(self, execution_results: Dict[str, str],   
                    target_table: str, 
                    execute_query: str, 
                    target_server: str):
        param = {
            "target_server": (target_server),
            "execute_query": (execute_query)
        }
        execution_results[target_table] = f'{Fore.BLUE}Executing Query{Style.RESET_ALL}'
        t = time()
        r = requests.post(self._api_endpoint_delete, json=param)
        pastebin_url = r.text
        result_ok, result_message = self._resultMessageValidate(pastebin_url)
        if result_ok == 1:
            result_message += f'({Fore.GREEN}{str(round(time()-t,2))} s){Style.RESET_ALL}'
        execution_results[target_table] =  result_message
        return pastebin_url

    def executeBulkInsert(self, execution_results: Dict[str, str],
                            origin_table: str, 
                            target_table: str, 
                            database_name: str, 
                            origin_server: str, 
                            target_server: str, 
                            origin_query: Optional[str] = None,
                            truncate_table: Optional[str] = 'true') -> str:
        """Responsible for execute Bulk Insert on API Server and get response
            Args:
                execution_results: Dictionary that will store response for this execution
                origin_table: Origin Table
                target_table: Target Table
                database_name: Database Name
                origin_server: Origin Server
                target_server: Target Server
                origin_query: Query used to make retreive data to bulk insert on target
                truncate: Truncate table before bulk insert ('true' or 'false')
            Returns:
                Pandas pd.DataFrame containing week code, year, week, and description
        """
        # data to be sent to api 
        param = {
            "origin_query": (origin_query),
            "origin_table": (origin_table),
            "target_table": (target_table),
            "database_name": (database_name),
            "origin_server": (origin_server),
            "target_server": (target_server),
            "truncate_table": (truncate_table)
        }

        t = time()
        execution_results[target_table] = f'{Fore.BLUE}Inserting Query{Style.RESET_ALL}'
        
        # sending post request and saving response as response object 
        r = requests.post(url = self._api_endpoint_bulk, json = param)
        
        # extracting response text  
        response_text = r.text

        result_ok, result_message = self._resultMessageValidate(response_text)
        if result_ok == 1:
            result_message += f'({Fore.GREEN}{str(round(time()-t,2))} s){Style.RESET_ALL}'
        execution_results[target_table] =  result_message
        return response_text

    @staticmethod
    def _resultMessageValidate(result_message: str):
        if len(result_message)==0:
            return 1, f'{Fore.GREEN}Sucess{Style.RESET_ALL}'
        else:
            if 'RowsCopied' in result_message:
                rowscopied = result_message[result_message.find(':',result_message.find('RowsCopied'))+2:result_message.find('\n',result_message.find('RowsCopied'))-1].strip()
                #elapsed_time = result_message[result_message.find(':',result_message.find('Elapsed'))+1:]
                return 1, f'{Fore.GREEN}Finished | {rowscopied} rows {Style.RESET_ALL}'
            elif "WARNING" in result_message:
                warning_message = result_message[result_message.find('|')+1:].strip()
                return 0, f'{Fore.RED}Erro | {warning_message}{Style.RESET_ALL}'
            else:
                #print(result_message)
                return 0, f'{Fore.RED}Unknown Error | {result_message}{Style.RESET_ALL}'