# Etap9. Odcinek: MapReduce: Wstpene przetworzenie danych - preprocessing

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRPreprocess(MRJob):

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        # domyslnie wszystko bylo stringami (bylo widac ze wyswietla w cudzyslowiach).
        # Dlatego month, day i distance zamieniamy na liczbowe
        month, day, distance = int(month), int(day), int(distance)
        # tylko takie kolumny chcemy.
        # Kluczem jest year, a wartosciami to co w krotce
        yield year, (month, day, airline, distance)

        # aby wynik zapisac do pliku csv to trzeba w terminalu wpisac:
        # python3 02_preprocessed_data.py flights.csv > preprocessed_data.csv

if __name__ == '__main__':
    MRPreprocess.run()