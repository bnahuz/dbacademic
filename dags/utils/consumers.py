import requests
import pandas as pd
import json

from time import sleep



class FileConsumer:
    def __init__(self, main_url, total: int = None ):
        self.main_url  =main_url
        self.total = total
        
    def request(self, **params ) -> pd.DataFrame:

            data_type : str = "json"
            n_tries : int = 5

            resource = params["resource"]
            if "data_type" in params:
                data_type = params["data_type"]

            

            self.url = f'{self.main_url}/{resource}'
            print(f'[INFO] - Getting data from {self.url} : data_type = {data_type}')
            if data_type == 'csv':
                sep = ","
                decimal = '.'

                if "sep" in params and 'decimal' in params:
                    sep = params["sep"]
                    decimal = params["decimal"]
                
                data = pd.read_csv(self.url, sep=sep, decimal=decimal, encoding='ISO-8859-1')
                
                
                
            elif data_type == 'json':
                headers = {
                    'Content-Type': 'application/json'
                }
                response = requests.get(self.url, headers=headers)
                for tries in range(n_tries):
                    if response.status_code == 200:
                        data = pd.read_json(self.url)                        
                    elif response.status_code == 500:
                        print(f"[ERROR] - Status Code 500: Internal Error, try number {tries + 1}, trying again.")
                        sleep(10)
                        continue
                    else:
                        raise Exception(f"[ERROR] - Status code {response.status_code}, {json.dumps(response)}")
                
            if "q" in params:
                #colunas_str = data.dtypes[data.dtypes == 'str'].index
                #data[colunas_str].fillna('Desconhecido', inplace=True) # todo
                data.fillna('Desconhecido', inplace=True) ## se for tudo string, pode dar erro
                data = data.query(params['q'])
                print (data)
                
            if self.total:
                data = data.iloc[:self.total] 

            return data  

class CkanConsumer:
    def __init__(self, main_url, total = 10):
        #params = params
        self.total = total

        
        self.url = f'{main_url}/api/action/datastore_search'
        

    def request(self, **params) -> pd.DataFrame:
        self.resource_id = params["resource_id"]
        if 'limit' not in params:
            limit = 10000
            if self.total < 1000:
                limit = self.total
            params['limit'] = limit
        if 'offset' not in params:
            params['offset'] =  0

        data = []

        
        #print (params)
        response = requests.get(self.url, params=params) # deu erro no univasf
        total = response.json()['result']['total']
        if total > self.total:
            total = self.total
        #print (response.json())
        print(f'[INFO] - Getting data from {self.url} on resource {self.resource_id} at offset {len(data)} from {total}')

        data.extend(response.json()['result']['records'])

        while len(data) < total:
            params['offset'] = len(data)
            print(f'[INFO] - Getting data from {self.url} on resource {self.resource_id} at offset {len(data)}')
            response = requests.get(self.url, params=params)
            while response.status_code != 200:
                print(f'[ERROR] - Response code {response.status_code} from {self.url} on resource {resource_id} at offset {len(data)}')
                print(f'[INFO] - Waiting 5 seconds before trying again')
                sleep(5)
                response = requests.get(self.url, params=params)
            data.extend(response.json()['result']['records'])
        
        print(f'[INFO] - Total of {len(data)} records retrieved from {self.url} on resource {self.resource_id}')
        #Reassembling the dataframe
        df = pd.DataFrame(data)
        return df