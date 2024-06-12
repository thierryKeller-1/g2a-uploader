import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from urllib3 import request
from dotenv import load_dotenv
import json
import os
import sys


COLUMN_ORDER = [
        'snapshot_date',
        'date_price',
        'date_debut',
        'date_fin',
        'prix_actuel',
        'prix_init',
        'n_offre',
        'Nb semaines',
        'url',
        'import_tag',
        'localite',
        'typologie',
        'cle_station',
        'nom_station',
        'nom',
        'Nb_personnes',
    ]

REQUIRED_COLUMN_ORDER = [
        'snapshot_date',
        'date_price',
        'date_debut',
        'date_fin',
        'prix_actuel',
        'prix_init',
        'n_offre',
        'Nb_semaines',
        'url',
        'import_tag',
        'localite',
        'typologie',
        'region_key',
        'region_name',
        'nom',
        'Nb_personnes',
    ]



def create_tag(website:str) -> str:
    return f"{website}-{datetime.now().strftime('%Y%m%d')}"

def remove_char(char:object):
    return str(char).replace(',', ' - ').replace('&', ' and ')

def format_data(data:list) -> object:
    global COLUMN_ORDER
    formated_data = []

    for x in data:
        result = {
            "snapshot_date" : x["snapshot_date"],
            "date_price" : x["date_price"],
            "date_debut" : x["date_debut"],
            "date_fin" : x["date_fin"],
            "prix_init" : x["prix_init"],
            "prix_actuel" : x["prix_actuel"],
            "n_offre" : x["n_offre"].replace('nan', ''),
            "Nb_semaines" : x["Nb_semaines"],
            "url" : x['url'].replace('www.campings.com', '').replace('www.maeva.com','').replace('www.booking.com','').replace('&', '$'),
            "import_tag" : x["import_tag"],
            "localite" : remove_char(x["localite"]),
            "typologie" : remove_char(x["typologie"]),
            "region_key" : x["region_key"],
            "region_name" : x["region_name"],
            "nom" : remove_char(x["nom"]),
            "Nb_personnes" : x["Nb_personnes"]
        }
        formated_data.append(result)
    return formated_data
    


class Uploader(object):

    load_dotenv()

    def __init__(self, website:str, freq:int, filename:str, date_snapshot:str, target:str='dev') -> None:
        self.website = website 
        self.freq = freq 
        self.filename = filename
        self.snapshotdate = date_snapshot
        self.target = target.lower()
        
        if not self.check_snapshotdate():
            sys.exit()
        self.setup_datasource()

    def setup_datasource(self):
        global COLUMN_ORDER
        self.data_source = pd.read_csv(f"{os.environ.get('STATIC_FOLDER_PATH')}/{self.filename}.csv", low_memory=False)
        self.data_source['import_tag'] = create_tag(self.website)
        columns = self.data_source.columns.to_list()
        not_in = list(set(COLUMN_ORDER).difference(columns))
        print(not_in)
        if not_in:
            for colum in not_in:
                self.data_source[colum] = ''
        self.data_source.rename(columns={'cle_station':'region_key', 'nom_station':'region_name', 'Nb semaines':'Nb_semaines'}, inplace=True)
        try:
            nb_semaines = [int(x) for x in self.data_source['Nb_semaines'].to_list()]
            self.data_source['Nb_semaines'] = nb_semaines
        except:
            pass
        n_offres = [str(x).replace('.0', '') for x in self.data_source['n_offre'].to_list()]
        self.data_source['n_offre'] = n_offres
        self.data_source.fillna(value='', inplace=True)
        self.data_source['snapshot_date'] = self.snapshotdate
        self.data_source = self.data_source[REQUIRED_COLUMN_ORDER]
        print(self.data_source.columns)

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

    def get_website_url(self, website_name:str) -> str:
        match(website_name.lower()):
            case 'booking':
                return "https://www.booking.com/"
            case 'maeva':
                return "https://www.maeva.com/"
            case 'campings':
                return "https://www.campings.com/"
            case 'edomizil':
                return 'https://www.e-domizil.ch/'
            case _:
                print('website name not reconized')
                sys.exit()
        
    def post(self, data:str) -> bool:     
 
        data = {
            "nights": self.freq,
            "website": self.website,
            "website_url": self.get_website_url(self.website),
            "data_content": data
        }
        print(data)

        encoded_data = json.dumps(data)
        try:
            response = request(
                method="POST",
                url= os.environ.get("DEV_ENDPOINT") if self.target == 'dev' else os.environ.get("PROD_ENDPOINT"),
                headers = {
                    'Authorization': f'Bearer {os.environ.get("G2A_DEV_TOKEN")}' if self.target == 'dev' else f'Bearer {os.environ.get("G2A_PROD_TOKEN")}'
                },
                body=encoded_data,
                timeout=60,
                retries=3
            )
            return response
        except:
            self.post(data)
    
    def upload(self):
        print(' ==> upload start!')

        index = 0
        post_data = []
        for x in range(self.history['last_index'], len(self.data_source)):
            new_data = self.data_source.iloc[x].fillna('').to_dict()
            post_data.append(new_data)
            if index == 10 or x >= len(self.data_source):
                post_data_formated = format_data(post_data)
                response = ''
                while True:
                    response = self.post(data=post_data_formated)
                    if response and response.status == 200:
                        break
                    else:
                        print('new attemp try')

                match(response.status):
                    case 200:
                        new_log = self.history
                        new_log['last_index'] = self.history['last_index'] + index
                        self.set_log(new_log)
                        post_data.clear()
                        index = 0
                    case _:
                        print(response.data)
                        sys.exit()
                new_log = self.history
                new_log['last_index'] = self.history['last_index'] + index
                self.set_log(new_log)
                post_data.clear()
                index = 0
            index += 1
        new_log = self.history
        new_log['last_index'] = self.history['last_index'] + index
        self.set_log(new_log)
        self.post(data=post_data_formated)
        print("  ==> data uploaded!")
