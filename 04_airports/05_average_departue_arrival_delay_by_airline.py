# Etap9. Odcinek: MapReduce: Srednie opoznienie odlotu i przylotu w rozbiciu na linie lotnicze

from mrjob.job import MRJob
from mrjob.step import MRStep

# Zapis do pliku:
# python3 05_average_departue_arrival_delay_by_airline.py flights.csv > average_delay_by_airline.csv
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

        if departure_delay == '':
            departure_delay = 0

        if arrival_delay == '':
            arrival_delay = 0

        departure_delay = float(departure_delay)
        arrival_delay = float(arrival_delay)

        yield airline, (departure_delay, arrival_delay)

    # key to airline
    def reducer(self, key, values):
        total_dep_delay = 0
        total_arr_delay = 0
        num_elements = 0
        for value in values:
            total_dep_delay += value[0]
            total_arr_delay += value[1]
            num_elements += 1
        yield key, (total_dep_delay / num_elements, total_arr_delay / num_elements)


if __name__ == '__main__':
    MRFlight.run()