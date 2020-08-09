import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object ScalaExample10Tasks extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  //  В этой задаче потребуется обработать логи событий GitHub при помощи Apache Spark.
  //  Исходные данные  можно скачать по ссылке:
  //  https://drive.google.com/file/d/0B9Rx0uhucsroYWJxdEpPd2JYcjg/view?usp=sharing
  //
  //  Язык реализации: scala
  //
  //  Задача:
  //  1. Загрузить файл с логами в Spark
  val outputPath = "./spark_output/ex"
  val path_github = "./datasource/ghtorrent-logs.txt"
  val githubLogs = sc.textFile(path_github)
  //  2. Вывести количество строк в файле
  println("Number line in file: " + githubLogs.count())
  //  3. Посчитать количество записей с уровнем WARNING
  githubLogs.filter(_.contains("WARN")).count()
  //  4. Посчитать, сколько всего репозиториев было обработано. Следует учитывать только вызовы api_client
  githubLogs.filter(_.contains("api_client")).count()
  //  5. Найти клиента, выполнившего больше всего HTTP вызовов

 val clientTopHttp = githubLogs.filter(_.contains(" http:")).flatMap (
    _.split(", ", -1) match {
      case Array(level, _, id,_*) => Try((id, level)).toOption
      case _ => None
    }
  ).map(obj => (obj._1.replace(" -- ","")
    .replaceAll(":.*", ""), obj._2))
    .groupByKey().mapValues(value => value.size)
    .sortBy(obj => obj._2, false)
    .first()
  println("Client witch most request: " + clientTopHttp)

  //  6. Найти клиента, с наибольшим количеством FAILED HTTP вызовов
 val htmlTopFailed = githubLogs.filter(x => x.contains("Failed") && !x.contains("https:"))
    .flatMap (
      _.split(", ", -1) match {
        case Array(level, _, id, _*) => Try((id, level)).toOption
        case _ => None
      }
    ).map(obj => (obj._1.replace(" -- ","")
   .replaceAll(":.*", ""), obj._2))
   .groupByKey().mapValues(value => value.size)
   .sortBy(obj => obj._2, false)
   .first()
  println("Most FAILED HTTP: " + htmlTopFailed)

  //  7. Найти наиболее активный по количеству вызовов час
 val mostActiveHour = githubLogs.flatMap(
   _.split(", ", -1) match {
     case Array(_, date, _*) => Try(
       "\\w[0-9]{2}:".r.findFirstIn(date)
     ).toOption
     case _ => None
   }
 ).map(d => ("[0-9]{2}".r.findFirstIn(d.toString).get, 1))
   .reduceByKey((dFirst, dNext) => dFirst + dNext)
   .sortBy(obj => obj._2, false)
   .map {
     case (key, value) => s"The most active hour: $key, Number of repetitions: $value"
   }
   .first()
  println(mostActiveHour)
  //  8. Найти наиболее активный репозиторий
 println(githubLogs.filter("Repo.*exists".r.findFirstIn(_).isDefined).flatMap(
    _.split(", ", -1) match {
      case Array(_,_, gitRepo) => Try(
        (gitRepo.replaceAll(".*Repo ", "").replaceAll(" exists.*", ""), 1)
      ).toOption
      case _ => None
    }
  ).reduceByKey((rFirst, rNext) => rFirst + rNext)
    .sortBy(obj => obj._2, false)
    .map {
      case (key, value) => s"The most active repository: $key, Number of repetitions: $value"
    }
    .first())
  //  9. Найти лидирующий по ошибкам Access Key (выполнить фильтрацию по "Access:")
  val topAccess = githubLogs.filter("Failed.*Access".r.findFirstIn(_).isDefined).flatMap(
    _.split(", ", -1) match {
      case Array(_, _, _, _, _, access, _*) => Try(
        (access.replaceAll(".*Access: ", ""), 1)
      ).toOption
      case _ => None
    }
  ).reduceByKey((accFirst, accNext) => accFirst + accNext)
    .sortBy(obj => obj._2, false)
    .map {
      case (key, value) => s"Access Key: $key, Number of repetitions: $value"
    }
  println("Top " + topAccess.first())
  topAccess.repartition(1).saveAsTextFile(outputPath + "access")

  //  10. Заказчик попросил посчитать количество успешных и неуспешных вызовов по интересующих его репозиториям. Необходимо скачать CSV файл с репозиториями, по которым требуется выполнить анализ и выполнить
  //  расчет: https://drive.google.com/file/d/0B9Rx0uhucsroRHNVTFpzMV9OUGs/view?usp=sharing

  val spark = SparkSession.builder()
    .appName("spark-example")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
    .format("csv")
    .option("header", "true")
    .load("./datasource/important-repos.csv")

  df.createOrReplaceTempView("rep")

  df.sqlContext.sql("SELECT * FROM rep").show()
  df.sqlContext.sql("SELECT id AS ID_Repository, owner_id, forked_from, created_at FROM rep ORDER BY created_at DESC LIMIT 10").show()

  sc.stop()
}
