"""
APARTADO 3.
Supongamos que los datos del grafo se encuentran repartidos en múltiples ficheros.
Queremos calcular los 3-ciclos, pero sólamente aquellos que sean locales a cada
uno de los ficheros.
Escribe un programa paralelo que calcule independientemente los 3-ciclos de cada
uno de los ficheros de entrada.
"""
import sys
from pyspark import SparkContext
sc = SparkContext()

# Función para hallar las listas de adyacencia de cada vértice según la pista 2
def agruparPor1erVertice(sc, filename): 
    aristas = aristasDistintasRDD(sc, filename)
    agrupacionesPor1erVertice = aristas.groupByKey()
    return agrupacionesPor1erVertice 

# Halla el rdd con las aristas del grafo 
def aristasDistintasRDD(sc, filename):
    return filename.map(arista).filter(lambda x: x != None).distinct()

# Devuelve la arista de una línea de fichero con los vértices ordenados lexicográficamente
def arista(linea):
    vertice1 = linea[0]
    vertice2 = linea[1]
    if vertice1 < vertice2:
         return (vertice1,vertice2)
    elif vertice1 > vertice2:
         return (vertice2,vertice1)
    else:
        pass # es vertice1 == vertice2 y no queremos incluir bucles
        
# Dada una tupla (vertice, listaAdyacencia), se le asigna la lista correspondiente de exists/pending
def existe_pending(tupla):
    vertice = tupla[0]
    listaAdyacencia = list(tupla[1])
    result = []
    for v in listaAdyacencia:
        if vertice <= v:
            result.append(((vertice,v),"exists"))
        else:
            result.append(((v,vertice),"exists"))
    for i in range(len(listaAdyacencia)):
        for j in range(i + 1, len(listaAdyacencia)):
            vertice1, vertice2 = listaAdyacencia[i], listaAdyacencia[j]
            if vertice1 <= vertice2:
                arista = vertice1, vertice2
            else:
                arista = vertice2, vertice1
            result.append((arista, ("pending",  vertice)))
    return result

def filtrar(tupla): # para filtrar las aristas
    lista = list(tupla[1])
    return "exists" in lista and lista!= ["exists"]*len(lista)

def triciclo(tupla): # tupla es de tipo (arista, existe) o (arista, (pending, vertice))
    result = []
    triciclo = [tupla[0][0],tupla[0][1]] # arista de la tupla
    for comp2 in list(tupla[1]):
        if comp2 != "exists": # comp2 = (pending, vertice)
            triciclo.append(comp2[1]) # vértice
            result.append(triciclo)
            triciclo = [tupla[0][0],tupla[0][1]] # para las siguientes iteraciones
    return result

def main(sc,filelist):
    rdd = sc.parallelize([])
    for file in filelist:
        filerdd = sc.textFile(file).map(lambda x: ((x[0], file),(x[2],file)))
        rdd = rdd.union(filerdd)
    result_aux = agruparPor1erVertice(sc,rdd).flatMap(existe_pending).groupByKey().filter(filtrar).flatMap(triciclo).collect()
    result = {}
    for archivos in result_aux:
        archivo = archivos[0][1]
        if archivo in result:
            ciclo = []
            for vertices in archivos:
                ciclo.append(vertices[0])
            result[archivo].append(tuple(ciclo))
        else:
            ciclo = []
            for vertices in archivos:
                ciclo.append(vertices[0])
            result[archivo]=[tuple(ciclo)]
    print(result)
    
if __name__ =="__main__":
    filelist = ["g0.txt","g1.txt"]
    if len(sys.argv) > 1:
        filelist = list(sys.argv[1][1:-1].split(","))
    main(sc,filelist)