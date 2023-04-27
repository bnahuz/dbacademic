import requests
import pandas as pd
from time import sleep


def filtro_chave_valor(dicionario, chave, valor):
    if chave in dicionario and dicionario[chave] and valor in dicionario[chave] :
        return True
    return False

class JSONConsumer:
    def __init__(self, main_url, total = 10):
        self.total = total
        self.main_url = main_url

    def request(self, **params) -> pd.DataFrame:
        resource = params["resource"]
        url = f"{self.main_url}/{resource}"
        data = requests.get(url).json()

        if "key" in params:
            key = params["key"]
            value = params["value"]
            data = list(filter(lambda d: filtro_chave_valor(d, key, value), data))
        df = pd.DataFrame(data[:self.total]) # nao sei se precisa disso
        return df 

    

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