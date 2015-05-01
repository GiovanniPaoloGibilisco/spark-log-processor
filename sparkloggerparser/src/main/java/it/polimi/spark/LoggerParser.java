package it.polimi.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerParser {

	static final Logger logger = LoggerFactory.getLogger(LoggerParser.class);

	public static void main(String[] args) throws IOException {

		Config.init(args);
		Config config = Config.getInstance();
		if (!Files.exists(Paths.get(config.inputFile))) {
			logger.error("Input file does not exist");
			return;
		}
		SparkConf conf = new SparkConf().setAppName("logger-parser").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		DataFrame logsframe = sqlContext.jsonFile(config.inputFile);
		logsframe.cache();		
		logsframe.registerTempTable("events");
	
		
		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%TaskStart'").registerTempTable("taskStartInfos");
		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%TaskEnd'").registerTempTable("taskEndInfos");
		
		
		DataFrame taskDetails = sqlContext.sql("SELECT 	`start.Task Info.Task ID` AS id,"
				+ "				`start.Stage ID` AS stageID,"							
				+ "				`start.Task Info.Executor ID` AS executorID,"
				+ "				`start.Task Info.Host` AS host,"
				+ "				`finish.Task Type` AS type,"
				+ "				`finish.Task Info.Finish Time` - `start.Task Info.Launch Time` AS executionTime,"
				+ "				`finish.Task Metrics.Executor Run Time` AS executorRunTime,"
				+ "				`finish.Task Metrics.Executor Deserialize Time` AS executorDeserializerTime,"
				+ "				`finish.Task Metrics.Result Serialization Time` AS resultSerializationTime,"
				+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Write Time` AS shuffleWriteTime,"
				+ "				`finish.Task Metrics.JVM GC Time` AS GCTime,"				
				+ "				`finish.Task Metrics.Result Size` AS resultSize,"
				+ "				`finish.Task Metrics.Memory Bytes Spilled` AS memoryBytesSpilled,"
				+ "				`finish.Task Metrics.Disk Bytes Spilled` AS diskBytesSpilled,"
				+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written` AS shuffleBytesWritten,"
				+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Records Written` AS shuffleRecordsWritten,"
				+ "				`finish.Task Metrics.Input Metrics.Data Read Method` AS dataReadMethod,"
				+ "				`finish.Task Metrics.Input Metrics.Bytes Read` AS bytesRead,"
				+ "				`finish.Task Metrics.Input Metrics.Records Read` AS recordsRead,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Remote Blocks Fetched` AS shuffleRemoteBlocksFetched,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Local Blocks Fetched` AS shuffleLocalBlocksFetched,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Fetch Wait Time` AS shuffleFetchWaitTime,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Remote Bytes Read` AS shuffleRemoteBytesRead,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Local Bytes Read` AS shuffleLocalBytesRead,"
				+ "				`finish.Task Metrics.Shuffle Read Metrics.Total Records Read` AS shuffleTotalRecordsRead"
				+ "		FROM taskStartInfos AS start"
				+ "		JOIN taskEndInfos AS finish"
				+ "		ON `start.Task Info.Task ID`=`finish.Task Info.Task ID`");
		
		taskDetails.show();
		
		

	}

}
