import pandas as pd
import urllib3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from random import randint
import json
import os
import sys
import time


def create_tag(website:str) -> str:
    return f"{website}-{datetime.now().strftime('%Y%m%d')}"

def remove_char(char:object):
    return str(char).replace(',', '-').replace('&', ' and ')

def format_data(data:list, website:str) -> object:
    formated_data = ""
    columns = [
        'web-scraper-order',
        'date_price',
        'date_debut',
        'date_fin',
        'prix_init',
        'prix_actuel',
        'typologie',
        'n_offre',
        'nom',
        'localite',
        'date_debut-jour',
        'Nb semaines',
        'cle_station',
        'nom_station'
    ]

    for x in range(len(data)):
        result = ""

        for col in columns:
            try:
                result += f"{remove_char(data[x][col])},"
            except KeyError:
                result += ","

        if 'url' in data[x].keys():
            url = data[x]['url'].replace('www.campings.com', '').replace('www.maeva.com','').replace('www.booking.com','').replace('&', '$')
            result += f'{url},'
        else:
            result += ","

        result += create_tag(website)

        if len(result.split(',')) == 16:
            formated_data += f"{result};"

        else:
            with open('uncorrect.json', 'a', encoding='utf-8') as openfile:
                openfile.write(f"{result};\n")

    return formated_data[:-1]



class Uploader(object):

    load_dotenv()

    def __init__(self, website:str, freq:int, filename:str) -> None:
        self.website = website
        self.freq = freq
        self.filename = filename
        self.data_source = pd.read_csv(f"{os.environ.get('STATIC_FOLDER_PATH')}/{filename}.csv")
        nb_semaines = self.data_source['Nb semaines'].astype(int)
        self.data_source['Nb semaines'] = nb_semaines

    def create_log(self) -> None:
        print('==> creating log file')
        self.log_file = f"{os.environ.get('LOG_FOLDER_PATH')}/{self.filename}.json"
        if not Path(self.log_file).exists():
            with open(self.log_file, 'w') as openfile:
                log = {'last_index': 0}
                openfile.write(json.dumps(log))

    def set_log(self, log:dict) -> int:
        with open(self.log_file, 'w') as openfile:
            openfile.write(json.dumps(log))
        self.load_history()

    def load_history(self) -> None:
        if not Path(self.log_file).exists():
            print("log file not found")
            sys.exit()
        with open(self.log_file, 'r') as openfile:
            self.history = json.loads(openfile.read())
        print(f"last log {self.history}")

    def post(self, target:str, data:str) -> bool:
        http = urllib3.PoolManager(timeout=5)
        response = http.request(
            method="POST",
            url= os.environ.get("DEV_ENDPOINT") if target == 'dev' else os.environ.get("PROD_ENDPOINT"),
            headers = {
                'Authorization': f'Bearer {os.environ.get("G2A_DEV_TOKEN")}' if target == 'dev' else f'Bearer {os.environ.get("G2A_PROD_TOKEN")}'
            },
            fields={
                "nights": self.freq,
                "website_name": self.website,
                "data_content": data
            }
        )
        print('  ==> response \n')
        print(response.data)
        return response

    def upload(self, target:str):
        print(' ==> upload start!')
        index = 0
        post_data = []
        for x in range(self.history['last_index'], len(self.data_source)):
            new_data = self.data_source.iloc[x].fillna('').to_dict()
            post_data.append(new_data)
            post_data_formated = format_data(post_data, self.website)
            if index == 20 or x >= len(self.data_source):
                response = self.post(target=target, data=post_data_formated)
                match(response.status):
                    case 200:
                        new_log = self.history
                        new_log['last_index'] = self.history['last_index'] + index
                        self.set_log(new_log)
                    case _:
                        print(response.data)
                index = 0
                post_data.clear()
            index += 1
        self.post(target=target, data=post_data_formated)
        print("  ==> data uploaded!")
