import requests
import pandas as pd
from time import sleep
class CkanConsumer:
    def __init__(self, main_url):
        self.url = f'https://{main_url}/api/action/datastore_search'

    def request(self, resource_id) -> pd.DataFrame:
        params = {
            'limit': 1000,
            'offset': 0,
            'resource_id': resource_id
        }
        data = []

        print(f'[INFO] - Getting data from {self.url} on resource {resource_id} at offset {len(data)}')
        response = requests.get(self.url, params=params)
        total = response.json()['result']['total']
        data.extend(response.json()['result']['records'])

        while len(data) < total:
            params['offset'] = len(data)
            print(f'[INFO] - Getting data from {self.url} on resource {resource_id} at offset {len(data)}')
            response = requests.get(self.url, params=params)
            while response.status_code != 200:
                print(f'[ERROR] - Response code {response.status_code} from {self.url} on resource {resource_id} at offset {len(data)}')
                print(f'[INFO] - Waiting 5 seconds before trying again')
                sleep(5)
                response = requests.get(self.url, params=params)
            data.extend(response.json()['result']['records'])
        
        print(f'[INFO] - Total of {len(data)} records retrieved from {self.url} on resource {resource_id}')
        #Reassembling the dataframe
        df = pd.DataFrame(data)
        return df