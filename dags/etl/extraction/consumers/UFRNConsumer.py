import json
import requests
import pandas as pd
from time import sleep

class UFRN_Consumer:
    def __init__(self, endpoint_request):
        self.url = f'https://dados.ufrn.br/{endpoint_request}'
    def request(self, data_type : str = 'csv', n_tries : int = 5) -> pd.DataFrame:
            print(f'[INFO] - Getting data from {self.url}')
            if data_type == 'csv':
                data = pd.read_csv(self.url, sep=";", decimal=',')
                return data
            elif data_type == 'json':
                headers = {
                    'Content-Type': 'application/json'
                }
                response = requests.get(self.url, headers=headers)
                for tries in range(n_tries):
                    if response.status_code == 200:
                        data = pd.read_json(self.url)
                        return data
                    elif response.status_code == 500:
                        print(f"[ERROR] - Status Code 500: Internal Error, try number {tries + 1}, trying again.")
                        sleep(10)
                        continue
                    else:
                        raise Exception(f"[ERROR] - Status code {response.status_code}, {json.dumps(response)}")
                return None ##TODO