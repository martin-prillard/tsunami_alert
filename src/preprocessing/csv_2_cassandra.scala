import scala.collection.mutable.MutableList
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}


/***********************************************************************************************************/
val csv_input = "/home/martin/workspace/tsunami_alert/dataset/data_tsunami.csv"
val date_format = "yyyy-MM-dd HH:mm:ss,SSS"
val date_start = "2014-12-30 00:00:00,000"
val date_end = "2015-02-01 00:00:00,000"
val time_step:Int = 5000 //minutes
val key_space = "tsunami_project"
val name_table = "tsunami_table"
val cassandra_replication = 3
val level = Level.WARN
/***********************************************************************************************************/


// Logger
Logger.getLogger("org").setLevel(level)
Logger.getLogger("akka").setLevel(level)


/*
  from a date in minutes, return the right timeslot
*/
val minute_to_timeslot = (date:Int, time_start:Int, time_step:Int) => {
 	val timeslot:Int = (date - time_start) / time_step
	var res:Int = 0
	if (timeslot == 0) {
		res = timeslot * time_step + time_start
	} else {
		// (timeslot-1) because we want to contact phone numbers in the previous apart
		res = (timeslot-1) * time_step + time_start
	}
	res
}


/*
  convert timestamp to minutes
*/
val timestamp_to_minute = (date:String) => {
	val fmt = new SimpleDateFormat(date_format)
	// cast in minutes
	val res:Int = ((fmt.parse(date).getTime() / 1000).toInt / 60).toInt
	res
}


/*
  add geolocation for each users for each timeslots
*/
val add_timeslot = (logs:List[(String,Int)], timeslots:Range) => {
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
	new_logs.toVector
}


/*
  Create the keyspace and the table
*/
CassandraConnector(sc.getConf).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS " + key_space + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + cassandra_replication + " }")
    session.execute(s"CREATE TABLE IF NOT EXISTS " + key_space + "." + name_table + " (code_gsm TEXT, timeslot INT, phone LIST<INT>, PRIMARY KEY ((code_gsm), timeslot))")
    session.execute(s"TRUNCATE " + key_space + "." + name_table)
}

// list of timeslots according a range in descending order
val time_start:Int = timestamp_to_minute(date_start)
val time_end:Int = timestamp_to_minute(date_end)
val timeslots = Range(time_start, time_end, time_step).reverse

// read CSV file
val csv = sc.textFile(csv_input)


/*
  pre-process data and insert it into cassandra table
  transform : 2015-01-18 09:19:16,888;Yok_98;35.462635;139.774854;526198
*/ 
val by_timeslot_gsm = csv.map(_.split(";")) //split csv lines
	// phone, timeslot, gsm_code : (Sap_24, 848777,23705630)
	.map(x => ( (x(4).toInt), (x(1), minute_to_timeslot(timestamp_to_minute(x(0)), time_start, time_step)) ))
	// group by phone : (848777,CompactBuffer((Yok_46,23691680), (Sap_24,23705630), ...))
	.groupByKey() 
	// sort by timeslot in descending order : (848777,List((Sap_24,23705630), (Yok_46,23691680), ...))
	.mapValues(_.toList.sortBy(_._2).reverse) 
	// add missing timeslot for each phone : (848777,Vector((Sap_24,27000000), (Yok_46,27000010), ...))
	.mapValues(add_timeslot(_, timeslots))
	// change the key value, phone to timeslot and gsm_code : Vector(((Sap_24,27000000),848777), ((Yok_46,27000010),848777), ...)
	.map(x => (x._2.map(y => ((y._1, y._2), x._1))))
	// return each element line by line : ((Sap_24,27000000),848777)
	.flatMap(x => x)
	// group by key (timeslot and gsm_code) : ((Sap_24,27000000),CompactBuffer(848777, 872939, ...))
	.groupByKey()
	// transform in an TsunamiModel object
	.map(x => (x._1._1, x._1._2, x._2))
	// save to cassandra
	.saveToCassandra(key_space, name_table)


