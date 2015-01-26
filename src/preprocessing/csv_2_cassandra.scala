import org.apache.log4j.{Level, Logger}
import tsunamialert.util_time
import tsunamialert.util_geo
import com.datastax.spark.connector._

// Logger
val level = Level.WARN
Logger.getLogger("org").setLevel(level)
Logger.getLogger("akka").setLevel(level)

/* ------------------------------------------------------ Util time ------------------------------------------------------ */

import scala.collection.mutable.MutableList
import java.text.SimpleDateFormat;
import java.util.Date;


/*
  from a date in second, return the right timeslot
*/
def second_to_timeslot(date:Int, time_start:Int, time_step:Int) : Int = {
 	val timeslot:Int = (date - time_start) / time_step
	if (timeslot == 0) {
		return timeslot * time_step + time_start
	} else {
		// (timeslot-1) because we want to contact phone numbers in the previous apart
		return (timeslot-1) * time_step + time_start
	}
}


/*
  convert timestamp to seconds
*/
def timestamp_to_minute(date:String) : Int = {
	val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
	// cast in minutes
	val res = ((fmt.parse(date).getTime() / 1000).toInt / 60).toInt
	return res
}


/*
  add geolocation for each users for each timeslots
*/
def add_timeslot(logs:List[(String,Int)], timeslots:Range) : Vector[(String,Int)] = {
	val new_logs:MutableList[(String,Int)] = MutableList[(String,Int)]()
	val it = logs.iterator
	// get the most recent timeslot
	var lastTs:Int = 999999999
	// get the most recent GSM_code
	var lastGeo = ""
	if (it.hasNext) {
		val log = it.next()
		lastGeo = (log._1)
		lastTs = (log._2)
	}

	// for all timeslots range
	timeslots.foreach{ ts => ts:Int
		// get the most recent recent GSM_code
		if ((lastTs > ts) && (it.hasNext)) {
			val log = it.next()
			lastGeo = (log._1)
			lastTs = (log._2)
		}
		// add in the timeslots_user
		new_logs += ((lastGeo,ts))
	}
	return new_logs.toVector
}

/* ------------------------------------------------------ Util time ------------------------------------------------------ */

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.spark.rdd.RDD
import java.io.File

def merge(srcPath: String, dstPath: String): Unit =  {
	val hadoopConfig = new Configuration()
	val hdfs = FileSystem.get(hadoopConfig)
	FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
}


/*
  calculate the coordinates of the centers of different GSM Zones
*/
def generate_GSM_zone_coordinates_CSV(data_input:RDD[String], csv_output:String) = {
	val part_file = csv_output + ".part.csv"
	FileUtil.fullyDelete(new File(part_file))
	FileUtil.fullyDelete(new File(csv_output))

	data_input.map(_.split(";"))
	.map(v => (v(1), (v(2), v(3))))
	.groupByKey()
	.map(x => (x._1, x._2.take(1)))
	.saveAsTextFile(part_file)
	// merge csv files
	merge(part_file, csv_output)
}

/* ------------------------------------------------------ Cassandra ------------------------------------------------------ */

val time_start:Int = timestamp_to_minute("2014-12-30 00:00:00,000")
val time_end:Int = timestamp_to_minute("2015-02-01 00:00:00,000")
val time_step:Int = 5000 //minutes


/*
  list of timeslots according a range in descending order
*/
val timeslots = Range(time_start, time_end, time_step).reverse


/* TODO
val conf = new SparkConf(True)
    .set(“spark.cassandra.connection.host”, CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setMaster(SparkMaster)
    .setAppName(SparkAppName)

//Connect to the Spark cluster
lazy val sc = new SparkContext(conf)
*/

val key_space = "tsunami_project"
val name_Table = "tsunami_table"

/*
========================================
 Keyspace: tsunami_project
========================================
 Table: tsunami_table
----------------------------------------
 - code_gsm  : String (partition key column)
 - timeslot : Int (clustering column)
 - phone : List
*/
//val myTable = sc.cassandraTable[(String, Int, Iterable[(Int)])](key_space, name_Table)



/*
  mapper object with Cassandra model
*/
class TsunamiModel(code_gsm:String, timeslot:Int, phone:Iterable[(Int)])



/*
  read CSV file 
  since AWS  : sc.textFile("s3://...")
  since HDFS : sc.textFile("file:///<local-path>/normalfill.csv")
*/
val csv = sc.textFile("/home/martin/workspace/tsunami_alert/dataset/data_tsunami.csv")


/*
  generate a GSM_zone files with geolocation coordinates
*/
//generate_GSM_zone_coordinates_CSV(csv, "/home/martin/workspace/tsunami_alert/dataset/GSM_Coord.csv") //TODO uncomment if needed


/*
  pre-process data and insert it into cassandra table
  transform : 2015-01-18 09:19:16,888;Yok_98;35.462635;139.774854;526198
  to        : 
*/ 
val by_timeslot_gsm = csv.map(_.split(";")) //split csv lines
	// phone, timeslot, gsm_code : (Sap_24, 848777,23705630)
	.map(x => ( (x(4).toInt), (x(1), second_to_timeslot(timestamp_to_minute(x(0)), time_start, time_step)) ))
	// group by phone : (848777,CompactBuffer((Yok_46,23691680), (Sap_24,23705630), ...))
	.groupByKey() 
	// sort by timeslot in descending order : (848777,List((Sap_24,23705630), (Yok_46,23691680), ...))
	.mapValues(_.toList.sortBy(_._2).reverse) 
	// add missing timeslot for each phone : (848777,Vector((Sap_24,27000000), (Yok_46,27000010), ...))
	.mapValues(add_timeslot(_, timeslots))
	// change the key value, phone to timeslot and gsm_code : Vector(((Sap_24,27000000),848777), ((Yok_46,27000010),848777), ...)
	.map(x => (x._2.map(y => ((y._1, y._2), x._1)))) //TODO optimize it ?
	// return each element line by line : ((Sap_24,27000000),848777)
	.flatMap(x => x)
	// group by key (timeslot and gsm_code) : ((Sap_24,27000000),CompactBuffer(848777, 872939, ...))
	.groupByKey()
	// transform in an TsunamiModel object
	.map(x => new TsunamiModel(x._1._1, x._1._2, x._2))
	// save to cassandra TODO bulk by code_gsm
	.saveToCassandra(key_space, name_table)


