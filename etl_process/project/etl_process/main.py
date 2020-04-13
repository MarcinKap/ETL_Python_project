
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3

from etl_process.project.etl_process.process.etl_process_with_db import EtlProcessWithDb
from sqlite3 import connect
from argparse import ArgumentParser

worker_tablse_stmt = """
    CREATE TABLE IF NOT EXISTS worker (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(20),
        salary INTEGER
    )"""

insert_stmt = 'INSERT INTO worker (name, salary) VALUES(?,?)'
data_to_insert = [
    ('name1', 100),
    ('name2', 200),
    ('name3', 300),
    ('name4', 400),
    ('name5', 500),
    ('name6', 600)
]

def main():


    parser = ArgumentParser(description='To jest zadanie ETL z Pythona')
    parser.add_argument('--path', dest='path', type = str, required = True)
    # parser.add_argument('--triplets_sample_path', dest='triplets_sample', type = str, required = True)
    # parser.add_argument('--unique_tracks_path', dest='tripltes_sample', type = str, required = True)
    args = parser.parse_args()


    sciezka = 'my_db.db'


    # WLASCIWY PROGRAM
    print("\nMenu programu ETL\nProszę wpisać cyfrę")

    while True:

        print("1 - program ETL\n"
              "3 - exit\n"
              "\nInput: ")
        code = input()

        if code == '1':
            print('\nUruchamianie programu ETL...')
            etl_process_with_db = EtlProcessWithDb()
            etl_process_with_db.run()
        elif code == '3':
            sys.exit()
        else:
            print("Error - zła ")













if '__main__' == __name__:
    main()
