# Santander Pollution

En este proyecto se plantea el proceso desarrollado y los resultados obtenidos con el fin de calcular los niveles de contaminación en las diferentes regiones de Santander. Para ello se han utilizado los datos de [los sensores móviles del OpenData de Santander](http://datos.santander.es/dataset/?id=sensores-moviles), los cuales no están actualizados y no tienen datos muy significativos, pero son suficientes para tener unos pequeños datos de prueba.
Los regiones utilizadas en el proyecto son las siguientes:

0. Sardinero
1. Valdenoja, Cueto y Universidades
2. Santander centro
3. Monte
4. Peñacastillo
5. Albericia, Adarzo y Alisal

## Infraestructura y arquitectura del sistema

A pesar de que el dataset utilizado no está actualizado y tiene muy pocas entradas, se ha diseñado el sistema con la idea de recibir los datos de todos los sensores móviles en tiempo real. Por lo tanto, se ha optado por una arquitectura de procesado en Streaming ya que sería necesario procesar una gran cantidad de datos en tiempo real.
Se ha optado por elegir Apache Kafka como gestor de colas para que actúe como receptor de los datos de los sensores y a su vez dar elasticidad al sistema. 
Apache Storm ha sido elegido como el motor de procesado de los datos, el cual se encargará de filtrar y agregar los datos obtenidos del gestor de colas para su posterior persistencia.
Y, por último, se ha escogido Apache Cassandra como gestor de almacenamiento de los datos, ya que ofrece un buen rendimiento para insertar los datos de los sensores y tiene facilidad para ser particionada acorde al particionado que realice el motor de procesado.
Sin embargo, ya que el objetivo de este trabajo es únicamente de realizar un sistema de prueba, el sistema se ha desplegado en un único nodo, siendo este un ordenador portátil, por lo tanto, no existe ningún tipo de particionado.

## Diseño del software

El proyecto consta de dos módulos software:
- Un *Data Ingestor* que recive un fichero CSV y lo envia a Kafka en función de unos parámetros. El módulo se encuentra en [este repositorio](https://github.com/algorri94/streamingestor)
- El software encargado de procesar los datos, el cual se encuentra en este mismo proyecto de github.

### Filtrado, procesado y agregación de los datos
Con el fin de procesar los datos se ha creado un *Topology* en Storm compuesto por 1 *Spout* y 3+2*n *Bolts* siendo n el número de regiones.
- **Kafka Spout**: Recibe los datos de Kafka
- **Line Parser Bolt**: Parsear cada una de las tuplas recibidas del *Spout* anterior.
- **Filter Classifier Bolt**: Filtra las tuplas que contengan datos de sensores erróneos y clasifica las tuplas por región y ventana temporal (en este caso de 10 segundos).
- **Write Lecturas Bolt**: Escribe en Cassandra los datos de las lecturas de los sensores filtradas y clasificadas.
- **Aggregation Bolt**: Calcula las agregaciones de todos los valores de emisiones dentro de una misma ventana temporal. Existe uno por región.
- **Write Aggregation Bolt**: Escribe en Cassandra las agregaciones calculadas en el *Bolt* anterior. Existe uno por región.

Por lo tanto, el gráfo de ejecución del sistema sería el siguiente:

![Grafo](https://image.prntscr.com/image/mwKjihvtRMOdH3ste5ySbA.png)

## Resultados
Tras ejecutar la aplicación con el dataset mencionado, se obtienen las siguientes agregaciones en Cassandra:

![Resultados](https://image.prntscr.com/image/yUh5Zi3zQeKiQZn0G9p96w.png)
