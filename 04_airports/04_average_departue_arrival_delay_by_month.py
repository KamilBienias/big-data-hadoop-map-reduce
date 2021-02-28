# Etap9. Odcinek: MapReduce: Srednie opoznienie odlotu i przylotu w rozbiciu na miesiac

from mrjob.job import MRJob
from mrjob.step import MRStep

# tak uruchamiac w terminalu:
# python3 04_average_departue_arrival_delay_by_month.py flights.csv
# Zapis do pliku:
# python3 04_average_departue_arrival_delay_by_month.py flights.csv > average_delay.csv
class MRFlight(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        # zamienia nic na 0, po to zeby nie bylo bledu przy rzutowaniu na float
        if departure_delay == '':
            departure_delay = 0

        if arrival_delay == '':
            arrival_delay = 0

        # te 3 kolumny byly stringami
        departure_delay = float(departure_delay)
        arrival_delay = float(arrival_delay)
        month = int(month)

        # 0 z przodu zeby sortowal przy outpucie
        yield f'{month:02d}', (departure_delay, arrival_delay)

    # kluczami sa kolejne wartosci miesiaca, czyli np dla miesiaca 7
    # te wszystkie listy [-3.0, -20.0] [-1.0, 4.0] itd beda w jednej liscie list
    def reducer(self, key, values):
        # laczne opoznienie odlotow
        total_dep_delay = 0
        # laczne opoznienie przylotow
        total_arr_delay = 0
        num_elements = 0
        # value to lista dwuelementowa
        for value in values:
            # pierwszy element z tej listy
            total_dep_delay += value[0]
            # drugi element z tej listy
            total_arr_delay += value[1]
            num_elements += 1
        yield key, (total_dep_delay / num_elements, total_arr_delay / num_elements)


if __name__ == '__main__':
    MRFlight.run()