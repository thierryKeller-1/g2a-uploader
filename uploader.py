import pandas as pd
from pathlib import Path
from datetime import datetime
from urllib3 import request
from dotenv import load_dotenv
from random import randint
import json
import os
import sys
import time


COLUMN_ORDER = [
        'snapshot_date',
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
        'nom_station',
        'url'
    ]

def create_tag(website:str) -> str:
    return f"{website}-{datetime.now().strftime('%Y%m%d')}"

def remove_char(char:object):
    return str(char).replace(',', ' - ').replace('&', ' and ')

def format_data(data:list, website:str) -> object:
    global COLUMN_ORDER
    formated_data = ""

    for x in data:
        result = ""
        result += f"{x['snapshot_date']},{x['date_price']},{x['date_debut']},{x['date_fin']},{x['prix_init']},{x['prix_actuel']},"
        result += f"{remove_char(x['typologie'])},{x['n_offre'].replace('nan', '')},{remove_char(x['nom'])},{remove_char(x['localite'])},"
        result += f"{x['date_debut-jour']},{x['Nb semaines']},{x['cle_station']},{remove_char(x['nom_station'])},"
        result += f"{x['url'].replace('www.campings.com', '').replace('www.maeva.com','').replace('www.booking.com','').replace('&', '$')},"
        result += create_tag(website)

        if len(result.split(',')) == 16:
            formated_data += f"{result};"
        
        else:
            with open('uncorrect.json', 'a', encoding='utf-8') as openfile:
                openfile.write(f"{result};\n")
    print(formated_data[:-1])
    return formated_data[:-1]
    


class Uploader(object):

    load_dotenv()

    def __init__(self, website:str, freq:int, filename:str, date_snapshot:str, target:str='dev') -> None:
        self.website = website 
        self.freq = freq 
        self.filename = filename
        self.snapshotdate = date_snapshot
        self.target = target.lower()
        self.setup_datasource()

    def setup_datasource(self):
        global COLUMN_ORDER
        self.data_source = pd.read_csv(f"{os.environ.get('STATIC_FOLDER_PATH')}/{self.filename}.csv", low_memory=False)
        nb_semaines = [int(x) for x in self.data_source['Nb semaines'].to_list()]
        self.data_source['Nb semaines'] = nb_semaines
        n_offres = [str(x).replace('.0', '') for x in self.data_source['n_offre'].to_list()]
        self.data_source['n_offre'] = n_offres
        columns = self.data_source.columns.to_list()
        not_in = list(set(COLUMN_ORDER).difference(columns))
        if not_in:
            for colum in not_in:
                self.data_source[colum] = ''
        self.data_source.fillna(value='', inplace=True)
        self.data_source['snapshot_date'] = self.snapshotdate
        self.data_source = self.data_source[COLUMN_ORDER]

    def check_snapshotdate(self) -> bool:
        if datetime.strptime(self.snapshotdate, "%d/%m/%Y").isoweekday() == 6:
            print('snapshot date valid')
            return True
        print(' snapshot date invalid')
        return False

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
        
    def post(self, data:str) -> bool:        
        response = request(
            method="POST",
            url= os.environ.get("DEV_ENDPOINT") if self.target == 'dev' else os.environ.get("PROD_ENDPOINT"),
            headers = {
                'Authorization': f'Bearer {os.environ.get("G2A_DEV_TOKEN")}' if self.target == 'dev' else f'Bearer {os.environ.get("G2A_PROD_TOKEN")}'
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
    
    def upload(self):
        print(' ==> upload start!')
        global COLUMN_ORDER
        if not self.check_snapshotdate():
            print('date invalid')
            return

        index = 0
        post_data = []
        for x in range(self.history['last_index'], len(self.data_source)):
            new_data = self.data_source.iloc[x].fillna('').to_dict()
            post_data.append(new_data)
            if index == 10 or x >= len(self.data_source):
                post_data_formated = format_data(post_data, self.website)
                response = self.post(data=post_data_formated)
                match(response.status):
                    case 200:
                        new_log = self.history
                        new_log['last_index'] = self.history['last_index'] + index
                        self.set_log(new_log)
                        post_data.clear()
                        index = 0
                    case _:
                        print(response.data)
                new_log = self.history
                new_log['last_index'] = self.history['last_index'] + index
                self.set_log(new_log)
                post_data.clear()
                index = 0
            index += 1
        new_log = self.history
        new_log['last_index'] = self.history['last_index'] + index
        self.set_log(new_log)
        self.post(target=target, data=post_data_formated)
        print("  ==> data uploaded!")
