from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import time

class FlightsCount(MRJob):
    def mapper_flights(self, _, line):
        flight_data = line.split(",")
        if len(flight_data) < 9:
            yield "iata_data", (flight_data[0][1:-1], flight_data[1][1:-1], flight_data[2][1:-1], flight_data[3][1:-1])
        if len(flight_data) > 9:
            yield (flight_data[16], flight_data[17]), flight_data[26]

    def reducer1(self, route, weather_delays):
        if route != "iata_data":
            lista_delays = []
            for wd in weather_delays:
                if wd!= "NASDelay" and wd != "NA":
                    lista_delays.append(int(wd))
                elif wd == "NA":
                    lista_delays.append(0)
            if len(lista_delays) != 0:
                yield "delays", (sum(lista_delays)*1.0/len(lista_delays), route)
        else:
            lista = [element for element in weather_delays]
            yield route, lista

    def reducer2(self, tag, info):
        if tag == "delays":
            lista1 = sorted(info)
            lista2 = lista1[-10:]
            for cosa in lista2:
                yield "top10", cosa
                yield "iata", ["top_iata", cosa[1][0]]
                yield "iata", ["top_iata", cosa[1][1]]
        else:
            lista = [element for element in info]
            yield "iata", ["all_iata", (lista)]

    def reducer3(self, key, values):
        if key == "top10":
            lista = [element for element in values]
            yield "tag", (key, lista)
        else:
            lista = []
            values_list = [value for value in values]
            for element in values_list:
                if element[0] == "top_iata":
                    lista.append(element[1])
            #print (lista)
            for element in values_list:
                if element[0] == "all_iata":
                    for element2 in element[1][0]:
                        #print (element2)
                        if element2[0] in lista:
                            yield "tag", (element2[0], (element2[1], element2[2], element2[3]))

    def reducer4(self, key, values):
        values_list = [value for value in values]
        lista = []
        definitivo = []
        for value in values_list:
            if value[0] != "top10":
                lista.append(value)
            else:
                top_airports = value[1]
        for top_airport in top_airports:
            for codes in lista:
                if top_airport[1][0] == codes[0]:
                    delay_time = top_airport[0]
                    firts_airport = codes[1]
                if top_airport[1][1] == codes[0]:
                    second_airport = codes[1]
            yield delay_time, (firts_airport, second_airport)

    def steps(self):
        return [MRStep(mapper=self.mapper_flights, reducer=self.reducer1),
                MRStep(reducer=self.reducer2),
                MRStep(reducer=self.reducer3),
                MRStep(reducer=self.reducer4)]

if __name__ == '__main__':
    start_time = time.time()
    FlightsCount.run()
    print ("--- %s seconds ---" % (time.time() - start_time))