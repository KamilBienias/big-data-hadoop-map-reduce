# Etap12. Odcinek: Rozwiazanie: Ruch taksowek w rozbiciu na godziny

from mrjob.job import MRJob
from mrjob.step import MRStep

# zapis do pliku:
# python3 04_total_pickups_by_hour.py yellow_tripdata_2016-01.csv > total_pickups_by_hour.csv
class MRTaxi(MRJob):

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        # godzina odbioru przez taksowkarza to druga kolumna w pliku csv
        # split() oddziela date od godziny, tworzy liste hour i wybiera godzine.
        # Nastepnie bierze tylko dwa pierwsze znaki
        hour = tpep_pickup_datetime.split()[1][:2]

        yield hour, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRTaxi.run()