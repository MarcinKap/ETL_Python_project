import sys
import csv
import os
import sqlite3
import time
from abc import abstractmethod
from sqlite3 import connect
from argparse import ArgumentParser

unique_tracks_table = """
    CREATE TABLE IF NOT EXISTS unique_tracks (

        identyfikator_wykonania INTEGER ,
        identyfikator_utworu INTEGER ,
        nazwa_artysty VARCHAR(20),
        tytul_utworu VARCHAR(20)
    )"""
insert_stmt = 'INSERT INTO unique_tracks ' \
              '(identyfikator_wykonania,identyfikator_utworu,nazwa_artysty,tytul_utworu) ' \
              'VALUES(?,?,?,?)'

triplets_sample_table = """
    CREATE TABLE IF NOT EXISTS triplets_sample (

        identyfikator_użytkownika INTEGER ,
        identyfikator_utworu INTEGER ,
        data_odsłuchania_utworu DATETIME_INTERVAL_CODE
    )"""
insert_triplets_sample = 'INSERT INTO triplets_sample ' \
                         '(identyfikator_użytkownika,identyfikator_utworu,data_odsłuchania_utworu) ' \
                         'VALUES(?,?,?)'


# METODY Z UTIL
# FUNKCJE OD UNIQUE TRACKS


def convert_tracks_file_to_tracks(sciezka_do_bazy_danych, sciezka_do_unique_tracks):
    with open(sciezka_do_unique_tracks, "r", encoding="ISO-8859-1") as infile:
        with connect(sciezka_do_bazy_danych) as db_connector2:
            db_connector2.execute(unique_tracks_table)
            db_cursor = db_connector2.cursor()
            print('zapisywanie unique_tracks')
            for line in infile:
                line_elements = line.split("<SEP>")
                if len(line_elements) == 4:
                    db_cursor.execute(insert_stmt,
                                      [line_elements[0], line_elements[1], line_elements[2], line_elements[3]])


# KONIEC FUNKCJI OD UNIQUE TRACKS


# FUNKCJE OD TRIPLETS
def convert_triplet_file_to_triplet(sciezka_do_bazy_danych, sciezka_do_triplets_sample):
    with open(sciezka_do_triplets_sample, "r", encoding="ISO-8859-1") as infile:
        with connect(sciezka_do_bazy_danych) as db_connector2:
            db_connector2.execute(triplets_sample_table)
            db_cursor = db_connector2.cursor()
            print('zapisywanie triplets_sample')
            for line in infile:
                line_elements = line.split("<SEP>")
                if len(line_elements) == 3:
                    db_cursor.execute(insert_triplets_sample, [line_elements[0], line_elements[1], line_elements[2]])


def find_top_5_tracks_and_most_popular_artist(sciezka_do_bazy_danych):
    with connect(sciezka_do_bazy_danych) as db_connector2:

        db_connector2.execute(triplets_sample_table)
        db_cursor = db_connector2.cursor()

        print('Szukanie najpopularniejszych utworów')
        # szukanie utworów
        select = 'select tytul_utworu, count(*)  from triplets_sample JOIN unique_tracks ' \
                 'ON triplets_sample.identyfikator_utworu = unique_tracks.identyfikator_utworu  ' \
                 'group by tytul_utworu order by count(*) DESC LIMIT 5'

        rows = db_connector2.cursor().execute(select)
        print('Najpopularniejsze utwory:')
        for row in rows:
            print('Nazwa Piosenki:', row[0], end=' ')
            print('Ilość odtworzeń:', row[1])
        print("Szukanie najpopularniejszego artysty")

        db_cursor_2 = db_connector2.cursor().execute(' select nazwa_artysty, count(*)  '
                                                     'from [triplets_sample] JOIN unique_tracks '
                                                     'ON [triplets_sample].identyfikator_utworu = '
                                                     'unique_tracks.identyfikator_utworu  '
                                                     'group by nazwa_artysty order by count(*) DESC LIMIT 1')
        print('Najpopularniejszy artysta to:')
        print(db_cursor_2.fetchone())


def find_top_5_tracks(sciezka_do_bazy_danych):
    print('dugi watek')
    with connect(sciezka_do_bazy_danych) as db_connector2:
        db_cursor = db_connector2.cursor()
        db_connector2.execute(triplets_sample_table)
        rows = db_cursor.execute(' select identyfikator_utworu, count(*)  '
                                 'from triplets_sample t group by identyfikator_utworu '
                                 'order by count(*) DESC LIMIT 5')
        # rows = db_cursor.fetchall()
        print('Najpopularniejsze utwory')
        for row in rows:
            print('ID Piosenki', row[0])
            print('Ilość odtworzeń', row[1])
        print("Szukanie najpopularniejszego artysty")


def find_most_popular_artist(sciezka_do_bazy_danych):
    print('wykonuje sie osobny watek')
    with connect(sciezka_do_bazy_danych) as db_connector2:
        db_connector2.execute(triplets_sample_table)
        db_cursor_2 = db_connector2.cursor().execute(' select nazwa_artysty, count(*)  '
                                                     'from [triplets_sample] JOIN unique_tracks '
                                                     'ON [triplets_sample].identyfikator_utworu = '
                                                     'unique_tracks.identyfikator_utworu  '
                                                     'group by nazwa_artysty order by count(*) DESC LIMIT 1')
        most_popular_artist = db_cursor_2.fetchone()
        print('Najpopularniejszy artysta to:')
        print(most_popular_artist)


# KLASA RUNNABLE
class Runnable:
    @abstractmethod
    def run(self):
        pass


# # ##############################
# # GŁÓWNA KLASA WYWOŁUJĄCA METODY
# class EtlProcessWithDb(Runnable):
#     STEP_ONE_STRING = "Krok 1 - wczytywanie danych z unique_tracks.txt"
#     STEP_TWO_STRING = "Krok 2 - wczytywanie danych z triplets_sample_20p.txt"
#     STEP_THREE_STRING = "Krok 3 - szukanie artysty z najwieksza liczba odsluchan i 5 najpopularniejszych utworów"
#     STEP_FOUR_STRING = "Krok 4 - szukanie 5 najpopularnijszych utworów"
#     STEP_FIVE_STRING = "Krok 5 - wypisanie czasu operacji"
#
#     # self.__track_repository = TrackRepository()
#
#     def run(self):
#         # rozpoczecie odliczania
#         start = time.process_time()
#
#
#         # wczytywanie unique tracks
#         print(EtlProcessWithDb.STEP_ONE_STRING)
#         convert_tracks_file_to_tracks()
#         print('Dane z unique_tracks zapisane')
#
#         # # wczytywanie triplets_sample
#         print(EtlProcessWithDb.STEP_TWO_STRING)
#         convert_triplet_file_to_triplet()
#
#         print(EtlProcessWithDb.STEP_THREE_STRING)
#         # wyszukiwanie najpopularniejszych piosenek i najpopularniejszego artysty
#         find_top_5_tracks_and_most_popular_artist()
#
#         print(EtlProcessWithDb.STEP_FIVE_STRING)
#         # koniec odliczania
#         elapsed = (time.process_time() - start)
#
#         # wypisanie czasu
#         print("\nCzas pracy: " + str(elapsed) + '\n')


def main():
    parser = ArgumentParser(description='To jest zadanie ETL z Pythona, '
                                        'trzeba podać ścieżkę do bazy danych i plików unique tracks.txt oraz '
                                        'triplets_sample_20p.txt')
    parser.add_argument('--path', dest='path', type=str, required=True)
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
            # etl_process_with_db = EtlProcessWithDb()
            # etl_process_with_db.run()

            STEP_ONE_STRING = "Krok 1 - wczytywanie danych z unique_tracks.txt"
            STEP_TWO_STRING = "Krok 2 - wczytywanie danych z triplets_sample_20p.txt"
            STEP_THREE_STRING = "Krok 3 - szukanie artysty z najwieksza liczba odsluchan i 5 najpopularniejszych utworów"
            STEP_FIVE_STRING = "Krok 5 - wypisanie czasu operacji"

            # self.__track_repository = TrackRepository()

            sciezka_do_bazy_danych = 'my_db.db'
            sciezka_do_unique_tracks = 'unique_tracks.txt'
            sciezka_do_triplets_sample = 'triplets_sample_20p.txt'

            # rozpoczecie odliczania
            start = time.process_time()

            # wczytywanie unique tracks
            print(STEP_ONE_STRING)
            convert_tracks_file_to_tracks(sciezka_do_bazy_danych, sciezka_do_unique_tracks)
            print('Dane z unique_tracks zapisane')

            # # wczytywanie triplets_sample
            print(STEP_TWO_STRING)
            convert_triplet_file_to_triplet(sciezka_do_bazy_danych, sciezka_do_triplets_sample)

            print(STEP_THREE_STRING)
            # wyszukiwanie najpopularniejszych piosenek i najpopularniejszego artysty
            find_top_5_tracks_and_most_popular_artist(sciezka_do_bazy_danych)

            print(STEP_FIVE_STRING)
            # koniec odliczania
            elapsed = (time.process_time() - start)

            # wypisanie czasu
            print("\nCzas pracy: " + str(elapsed) + '\n')



        elif code == '3':
            sys.exit()
        else:
            print("Error - zły numer")


if '__main__' == __name__:
    main()
