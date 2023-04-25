import requests
import pandas as pd
from time import sleep
class CkanConsumer:
    def __init__(self, **kwargs):
        main_url = kwargs['main_url']
        resource_id = kwargs['main_url']

        self.resource_id = resource_id
        self.url = f'{main_url}/api/action/datastore_search'
        

    def request(self) -> pd.DataFrame:
        params = {
            'limit': 10,  #1000,
            'offset': 0,
            'resource_id': self.resource_id
        }
        data = []

        print(f'[INFO] - Getting data from {self.url} on resource {self.resource_id} at offset {len(data)}')
        response = requests.get(self.url, params=params)
        #total = response.json()['result']['total']
        total = 10
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