import java.io.File
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}


/***********************************************************************************************************/
val csv_input  = "/home/martin/workspace/tsunami_alert/dataset/data_tsunami.csv"
val csv_gsm_coord = "/home/martin/workspace/tsunami_alert/dataset/GSM_Coord.csv"
val level = Level.WARN
/***********************************************************************************************************/


// Logger
Logger.getLogger("org").setLevel(level)
Logger.getLogger("akka").setLevel(level)


/*
  merge result files in one
*/
val merge = (srcPath: String, dstPath: String) => {
	val hadoopConfig = new Configuration()
	val hdfs = FileSystem.get(hadoopConfig)
	FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
}


/*
  calculate the coordinates of the centers of different GSM Zones
*/
val generate_GSM_zone_coordinates_CSV = (csv:RDD[String], csv_output:String) => {

	val part_file = csv_output + ".hdfs"
	// remove last results
	FileUtil.fullyDelete(new File(csv_output))

	csv.map(_.split(";"))
	.map(v => (v(1), (v(2), v(3))))
	.groupByKey()
	.map(x => (x._1, x._2.take(1)))
	.flatMap { case (k,values) => values.map(k -> _) }
	.map(tuple => "%s;%s;%s".format(tuple._1, tuple._2._1, tuple._2._2))
	.saveAsTextFile(part_file)

	// merge csv files
	merge(part_file, csv_output)
	// remove temp directory
	FileUtil.fullyDelete(new File(part_file))
}


/*
  generate a GSM_zone files with geolocation coordinates
*/
val csv = sc.textFile(csv_input)
generate_GSM_zone_coordinates_CSV(csv, csv_gsm_coord)

