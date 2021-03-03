# Etap10. Odcinek: Srednia calkowita kwota rachunku w rozbiciu na dostawcow

from mrjob.job import MRJob
from mrjob.step import MRStep

# uruchamia
# python3 01_average_total_amount_by_vendor.py yellow_tripdata_2016-01.csv
# wyjdzie ze dwoch dostawcow ma takie srednie kwoty na kurs
# "1"    15.46
# "2"    15.79
class MRMap(MRJob):

    def mapper(self, _, line):
        # kolumny z naglowkami usunal z oryginalnego csv. Tak sie latwiej przetwarza dane
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        # klucz to id dostawcy, a vartosc to calkowity koszt podrozy
        yield VendorID, float(total_amount)

    def reducer(self, key, values):
        total = 0
        num = 0
        for value in values:
            total += value
            num += 1
        yield key, total / num

if __name__ == '__main__':
    MRMap.run()