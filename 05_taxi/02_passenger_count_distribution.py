# Etap10. Odcinek: Zadanie: Znalezc rozklad liczby pasazerow na przejazd

from mrjob.job import MRJob
from mrjob.step import MRStep

# zapis do pliku
# python3 02_passenger_count_distribution.py yellow_tripdata_2016-01.csv > passenger_count.csv
class MRDistribution(MRJob):

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        # kluczem jest ilosc pasazerow w jednym przejezdzie
        yield passenger_count, 1

    # sumuje ile jest przejazdow o podanej w kluczu liczbie pasazerow
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRDistribution.run()