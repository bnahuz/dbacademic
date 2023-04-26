import requests
import pandas as pd
from time import sleep
class CkanConsumer:
    def __init__(self, main_url, total, **params):
        self.params = params
        self.total = total

        self.resource_id = params["resource_id"]
        self.url = f'{main_url}/api/action/datastore_search'
        

    def request(self) -> pd.DataFrame:
        if 'limit' not in self.params:
            limit = 10000
            if self.total < 1000:
                limit = self.total
            self.params['limit'] = limit
        if 'offset' not in self.params:
            self.params['offset'] =  0

        data = []

        
        #print (self.params)
        response = requests.get(self.url, params=self.params)
        total = response.json()['result']['total']
        if total > self.total:
            total = self.total
        #print (response.json())
        print(f'[INFO] - Getting data from {self.url} on resource {self.resource_id} at offset {len(data)} from {total}')

        data.extend(response.json()['result']['records'])

        while len(data) < total:
            self.params['offset'] = len(data)
            print(f'[INFO] - Getting data from {self.url} on resource {self.resource_id} at offset {len(data)}')
            response = requests.get(self.url, params=self.params)
            while response.status_code != 200:
                print(f'[ERROR] - Response code {response.status_code} from {self.url} on resource {resource_id} at offset {len(data)}')
                print(f'[INFO] - Waiting 5 seconds before trying again')
                sleep(5)
                response = requests.get(self.url, params=self.params)
            data.extend(response.json()['result']['records'])
        
        print(f'[INFO] - Total of {len(data)} records retrieved from {self.url} on resource {self.resource_id}')
        #Reassembling the dataframe
        df = pd.DataFrame(data)
        return df