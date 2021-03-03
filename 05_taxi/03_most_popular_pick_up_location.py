# Etap10. Odcinek: MapReduce: Najbardziej popularna lokalizacja odbioru

from mrjob.job import MRJob
from mrjob.step import MRStep

# czas uruchomienia lokalnie: 8 min 20 sek

# polecenie do uruchomienia w chmurze AWS
# python .\03_most_popular_pick_up_location.py -r emr s3://big-data-hadoop/data/* --output-dir=s3://big-data-hadoop/output/job1
# python .\03_most_popular_pick_up_location.py -r emr --num-core-instances 4 s3://big-data-hadoop/data/* --output-dir=s3://big-data-hadoop/output/job2

# uruchomienie lokalnie:
# python3 03_most_popular_pick_up_location.py yellow_tripdata_2016-01.csv
# zapis do pliku:
# python3 03_most_popular_pick_up_location.py yellow_tripdata_2016-01.csv > most_popular_pick_up_location.csv
class MRTaxi(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer_get_sorted)
        ]

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')

        # bierze ze stringa 8 poczatkowych znakow, zeby byla mniejsza dokladnosc
        # zeby bardziej prawdopodobne bylo znalezienie dwoch takich samych lokalizacji
        pickup_latitude = pickup_latitude[:8]
        # dodatkowy znak na minus
        pickup_longitude = pickup_longitude[:9]

        yield (float(pickup_latitude), float(pickup_longitude)), 1

    def reducer(self, key, values):
        yield key, sum(values)

    def mapper_get_keys(self, key, value):
        yield None, (value, key)

    def reducer_get_sorted(self, key, values):
        self.results = []
        for value in values:
            self.results.append((key, value))
        # zwraca liste posortowana od najwiekszego do najmniejszego
        yield None, sorted(self.results, reverse=True)[:10]

if __name__ == '__main__':
    MRTaxi.run()