import sys
import os
from multiprocessing.dummy import Pool
from sqlite3 import connect

import threading

# read unique_tracks.txt file and convert to list of track objects


unique_tracks_table = """
    CREATE TABLE IF NOT EXISTS unique_tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identyfikator_wykonania INTEGER ,
        identyfikator_utworu INTEGER ,
        nazwa_artysty VARCHAR(20),
        tytul_utworu VARCHAR(20)
    )"""
insert_stmt = 'INSERT INTO unique_tracks (identyfikator_wykonania,identyfikator_utworu,nazwa_artysty,tytul_utworu) VALUES(?,?,?,?)'


triplets_sample_table= """
    CREATE TABLE IF NOT EXISTS triplets_sample (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identyfikator_użytkownika INTEGER ,
        identyfikator_utworu INTEGER ,
        data_odsłuchania_utworu DATETIME_INTERVAL_CODE
    )"""
insert_triplets_sample = 'INSERT INTO triplets_sample (identyfikator_użytkownika,identyfikator_utworu,data_odsłuchania_utworu) VALUES(?,?,?)'


# FUNKCJE OD UNIQUE TRACKS
def convert_tracks_file_to_tracks():
    with open("resources/unique_tracks.txt", "r", encoding="ISO-8859-1") as infile:

        with connect('my_db.db') as db_connector2:
            db_connector2.execute(unique_tracks_table)
            db_cursor = db_connector2.cursor()
            for line in infile:
                line_elements = line.split("<SEP>")
                if(len(line_elements) == 4):
                    db_cursor.execute(insert_stmt, [line_elements[0], line_elements[1], line_elements[2], line_elements[3]])

# convert splitted line to track object
# def convert_line_elements_to_track(line_elements):
#
#     # ZAMIANA NA TRACK ID ITD I DODAWANIE DO BAZY DANYCH
#     performance_id = line_elements[0].strip()
#     track_id = line_elements[1].strip()
#     artist_name = line_elements[2].strip()
#     track_name = line_elements[3].strip()
#
#     return Track(track_id, performance_id, artist_name, track_name, 0)

# def insert_track_into_table(data_to_insert):
#     with connect('my_db.db') as db_connector2:
#         db_connector2.execute(unique_tracks_table)
#         db_cursor = db_connector2.cursor()
#         db_cursor.executemany(insert_stmt, data_to_insert)

# KONIEC FUNKCJI OD UNIQUE TRACKS


# FUNKCJE OD TRIPLETS
def convert_triplet_file_to_triplet():
    with open("resources/triplets_sample_20p.txt", "r", encoding="ISO-8859-1") as infile:
        with connect('my_db.db') as db_connector2:
            db_connector2.execute(triplets_sample_table)
            db_cursor = db_connector2.cursor()
            for line in infile:
                line_elements = line.split("<SEP>")
                if(len(line_elements) == 3):
                    db_cursor.execute(insert_triplets_sample, [line_elements[0], line_elements[1], line_elements[2]])
        print('zapisywanietriplets_sample, liczba obiektów:')


def convert_line_elements_to_triplet(line_elements):
    user_id = line_elements[0].strip()
    track_id = line_elements[1].strip()
    date_of_listening = line_elements[2].strip()
    return Triplet(user_id, track_id, date_of_listening)
# KONIEC


def convert_line_elements_to_listen(line_elements):
    user_id = line_elements[0].strip()
    track_id = line_elements[1].strip()
    listen_date = line_elements[2].strip()
    return Listen(user_id, track_id, listen_date)


def find_top_5_tracks_and_most_popular_artist():
    with connect('my_db.db') as db_connector2:
        db_connector2.execute(triplets_sample_table)
        db_cursor = db_connector2.cursor()
        # t = threading.Thread(target=find_most_popular_artist(), daemon = True)
        # t2 = threading.Thread(target=find_top_5_tracks(), daemon = True)
        # t.start()
        # t2.start()
        print('Szukanie najpopularniejszych utworów')
        # szukanie utworów
        db_cursor.execute(' select identyfikator_utworu, count(*)  from triplets_sample t group by identyfikator_utworu order by count(*) DESC LIMIT 5')
        rows=db_cursor.fetchmany()
        print('Najpopularniejsze utwory')
        for row in rows:
            print('ID Piosenki', row [0])
            print('Ilość odtworzeń',row [1])
        print("Szukanie najpopularniejszego artysty")
        # szukanie artysty
        db_cursor_2 = db_connector2.cursor().execute(' select nazwa_artysty, count(*)  from [triplets_sample] JOIN unique_tracks ON [triplets_sample].identyfikator_utworu = unique_tracks.identyfikator_utworu  group by nazwa_artysty order by count(*) DESC LIMIT 1')
        most_popular_artist = db_cursor_2.fetchone()
        print('Najpopularniejszy artysta to:')
        print(most_popular_artist)

def find_top_5_tracks():
    print('dugi watek')
    with connect('my_db.db') as db_connector2:
        db_cursor = db_connector2.cursor()
        db_connector2.execute(triplets_sample_table)
        db_cursor.execute(' select identyfikator_utworu, count(*)  from triplets_sample t group by identyfikator_utworu order by count(*) DESC LIMIT 5')
        rows = db_cursor.fetchall()
        print('Najpopularniejsze utwory')
        for row in rows:
            print('ID Piosenki', row[0])
            print('Ilość odtworzeń', row[1])
        print("Szukanie najpopularniejszego artysty")

def find_most_popular_artist():
    print('wykonuje sie osobny watek')
    with connect('my_db.db') as db_connector2:
        db_connector2.execute(triplets_sample_table)
        db_cursor_2 = db_connector2.cursor().execute(' select nazwa_artysty, count(*)  from [triplets_sample] JOIN unique_tracks ON [triplets_sample].identyfikator_utworu = unique_tracks.identyfikator_utworu  group by nazwa_artysty order by count(*) DESC LIMIT 1')
        most_popular_artist = db_cursor_2.fetchone()
        print('Najpopularniejszy artysta to:')
        print(most_popular_artist)