import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MBA {

  /*
  Poner en funcionamiento un cluster en Spark
  1) Acceder a la carpeta Spark
  2) Acceder a la carpeta sbin
  3) Iniciamos el master
        ./start-master.sh
  4) Accedemos en el navegador a 127.0.0.1:8080 y copiamos la url del master spark://...
  5) Para añadir esclavos al cluster
        ./start-worker.sh spark://adrian-mesa:7077
  Manejo de la consola de Scala en Spark:
  1) Iniciamos la shell de Scala accediendo a /bin dentro de Spark e introducimos:
         ./spark-shell --master spark://adrian-mesa:7077

  */

  def main(args : Array[String]): Unit = {

    // Creamos la configuración de Spark
    val sparkConf = new SparkConf()
      .setMaster("spark://adrian-mesa:7077")
      .setAppName("MBA")


    val sc = new SparkContext(sparkConf)

    // Creamos un RDD de nombre dataset e introducimos los datos a analizar
    val dataset = sc.textFile("/home/adrimc97/Descargas/data/datos.txt")

    // Para comprobar estado del dataset: dataset.take(20).foreach(println)

    // Primer paso: ordenamos los datos alfabéticamente para evitar duplicados
    // en la generación de tuplas
    val sorted = dataset.map(line => line.split(" ").toList).map(_.sorted)

    // Segundo paso: generamos combinaciones de los elementos en tuplas donde
    // un elemento es una tupla de dos productos y el otro sera la clave 1

    val tuplas = sorted.map(tupla => tupla.combinations(2).toList)
    // Para pasar de lista a tuplas
    val onlytuplas = tuplas.map(_.map(_ match{case List(a,b) => (a,b)}))
    // Generamos nuevas tuplas de la forma ((String,String),1)
    val tuplasb = onlytuplas.map(_.map(x => (x,1)))

    // Transformo el rdd(array) a lista para poder unir todas las listas
    val listtr = tuplasb.collect().toList
    val flatenned = listtr.flatten

    // Ahora tenemos una sola lista con todas las tuplas,
    // pasamos la lista a rdd para reducirlo
    val rddtuplas = spark.sparkContext.parallelize(flatenned)

    // Realizamos el reducebykey para unir las tuplas con los mismos resultados
    val rddcom = rddtuplas.reduceByKey(_ + _)

    // Por último imprimimos por pantalla el número de resultados que queramos
    rddcom.sortBy(_._2, false).take(10).foreach(println)

    // Juntamos las 16 particiones generadas en una
    val rddFinal = rddcom.coalesce(1)

    // Finalmente guardamos el archivo en local como .txt
    rddFinal.saveAsTextFile("/home/adrimc97/Descargas/Resultado_Final")
  }


}
