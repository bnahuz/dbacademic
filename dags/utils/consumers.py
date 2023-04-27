import requests
import pandas as pd
from time import sleep
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
        response = requests.get(self.url, params=params,  verify=False) # deu erro no univasf
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