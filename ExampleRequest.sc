import org.apache.spark.sql.functions.{col, from_unixtime, min}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val sparkConf = new SparkConf()
  .setAppName("spark-example")
  .setMaster("local[*]")
val sc = new SparkContext(sparkConf)
val sparkSession = SparkSession
  .builder
  .appName("test")
  .getOrCreate()

val login_activity_type_id = 0
val logout_activity_type_id = 1

case class User(user_id: Int, user_name: String)

val users = Seq(User(1, "Doe, John"), User(2, "Deo, June"), User(3, "Dow, Johnes"))
case class UserEvent(user_id: Int, activity_id: Int, timestamp: Long)

val user_events = List(
UserEvent(1, login_activity_type_id, 151476480000L),
UserEvent(2, login_activity_type_id, 1514808000000L),
UserEvent(1, logout_activity_type_id, 1514829600000L),
UserEvent(1, login_activity_type_id, 1514894400000L)
)

val users_df = sparkSession.sqlContext.createDataFrame(sc.parallelize(users))
users_df.createOrReplaceTempView("users")

val user_events_df = sparkSession.sqlContext.createDataFrame(sc.parallelize(user_events))
user_events_df.createOrReplaceTempView("user_events")

val user_events_ss = sparkSession.sql("FROM user_events")
  .select(from_unixtime(col("timestamp") / 3600).as("date"), col("user_id"),col("activity_id"))

val users_ss = sparkSession.sql("FROM users")
  .select(col("*"))

val result = users_ss.join(user_events_ss, users_ss("user_id") === user_events_ss("user_id"))
  .filter(col("activity_id") === 0)
  .select(users_ss.col("user_id"), col("activity_id"), col("date"))
  .groupBy("user_id", "activity_id").agg(min("date").as("min_data_login"))

result.show()

sc.stop()
sparkSession.stop()


/*
user_events_df.createOrReplaceTempView('user_events')

result = ss.sql(f"""
WRITE QUERY HERE
""")

result.show()*/