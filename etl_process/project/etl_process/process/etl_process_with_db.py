# from etl_process.project.etl_process.track.track_repository import TrackRepository
from etl_process.project.etl_process.utils import util as utils
import time
from etl_process.project.etl_process.utils.runnable import Runnable
from etl_process.project.etl_process.utils.util import find_top_5_tracks_and_most_popular_artist


class EtlProcessWithDb(Runnable):
    STEP_ONE_STRING = "Krok 1 - wczytywanie danych z unique_tracks.txt"
    STEP_TWO_STRING = "Krok 2 - wczytywanie danych z triplets_sample_20p.txt"
    STEP_THREE_STRING = "Krok 3 - szukanie artysty z najwieksza liczba odsluchan i 5 najpopularniejszych utworów"
    STEP_FOUR_STRING = "Krok 4 - szukanie 5 najpopularnijszych utworów"
    STEP_FIVE_STRING = "Krok 5 - wypisanie czasu operacji"

        # self.__track_repository = TrackRepository()



    def run(self):
        # rozpoczecie odliczania
        start = time.process_time()

        # wczytywanie unique tracks
        print(EtlProcessWithDb.STEP_ONE_STRING)
        utils.convert_tracks_file_to_tracks()
        print('Dane z unique_tracks zapisane')

        # # wczytywanie triplets_sample
        print(EtlProcessWithDb.STEP_TWO_STRING)
        utils.convert_triplet_file_to_triplet()


        print(EtlProcessWithDb.STEP_THREE_STRING)
        # wyszukiwanie najpopularniejszych piosenek i najpopularniejszego artysty
        find_top_5_tracks_and_most_popular_artist()

        print(EtlProcessWithDb.STEP_FIVE_STRING)
        # koniec odliczania
        elapsed = (time.process_time() - start)

        # wypisanie czasu
        print("\nCzas pracy: " + str(elapsed)+ '\n')
