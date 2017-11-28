from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import time

class Flight_Diverted(MRJob):
    def mapper_flights(self, _, line):
        flight_data = line.split(",")
        if flight_data[23] == "1":
            yield flight_data[1], flight_data[23]

    def reducer1(self, month, diverted):
        if month != "Month":
            lista_diverted = [x for x in diverted]
            total = len(lista_diverted)
            if month == "1":
                month = "Enero"
            elif  month == "2":
                month = "Febrero"
            elif  month == "3":
                month = "Marzo"
            elif  month == "4":
                month = "Abril"
            elif  month == "5":
                month = "Mayo"
            elif  month == "6":
                month = "Junio"
            elif  month == "7":
                month = "Julio"
            elif  month == "8":
                month = "Agosto"
            elif  month == "9":
                month = "Septiembre"
            elif  month == "10":
                month = "Octubre"
            elif  month == "11":
                month = "Noviembre"
            elif  month == "12":
                month = "Diciembre"
            yield month, total

    def steps(self):
        return [MRStep(mapper=self.mapper_flights), MRStep(reducer=self.reducer1)]

if __name__ == '__main__':
    start_time = time.time()
    Flight_Diverted.run()
    print "--- %s seconds ---" % (time.time() - start_time)
