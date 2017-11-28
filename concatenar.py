# -*- coding: utf-8 -*-

concatenado = open("concatenado.csv", 'w')
archivos = ['2006', '2007', '2008']

for archivo in archivos:
    archivo_t = open(archivo+".csv", 'r')
    print archivo+".csv"
    archivo_t.readline()
    for line in archivo_t:
        concatenado.write(line)
    archivo_t.close()

concatenado.close()
