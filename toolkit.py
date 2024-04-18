import argparse
from datetime import datetime, timedelta


def main_arguments() -> object:
    parser = argparse.ArgumentParser(description="G2A uploader program",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--website', '-w', dest='website', default='', help="Nom du plateforme des donnees de scrap")
    parser.add_argument('--frequency', '-f', dest='frequency',
                        default='', help="frequence de nuitee de scrap")
    parser.add_argument('--name', '-n', dest='name',
                        default='', help="Le nom du fichier source sans extension")
    parser.add_argument('--target', '-t', dest='target',
                        default='dev', help="API dev ou prod")
    parser.add_argument('--date-snapshot', '-d', dest='snapshotdate', help="Le samedi de la semaine de scrap")
    return parser.parse_args()


ARGS_INFO = {
        '-w': {'long': '--website', 'dest': 'website', "help": "Nom du plateforme source des donnees"},
        '-f': {'long': '--frequency', 'dest': 'frequency', "help": "frequence de nuitee"},
        '-n': {'long': '--name', 'dest': 'name', "help": "Nom du fichier source"},
        '-t': {'long': '--target', 'dest': 'target', "help": "API a utiliser, dev ou prod"},
        '-d': {'long': '--date-snapshot', 'dest': 'snapshotdate', "help": "saturday of week scrap"}
    }

def check_arguments(args, required):
    miss = []

    for item in required:
        if not getattr(args, ARGS_INFO[item]['dest']):
            miss.append(
                f'{item} ou {ARGS_INFO[item]["long"]} ({ARGS_INFO[item]["help"]})')
    return miss