package it.polimi.spark.estimator;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Estimate the duration of an application on a given dataset size starting from
 * some runs on smaller data set sizes The estimation first tries to build a
 * model to estimate the growth of each stage and then aggregates the expected
 * stage application according to the estimateJobduration utility function (in
 * Utils.java)
 *
 */
public class ApplicationEstimator {
	long applicationDurationEstimation = 0;
	Map<Integer, Long> jobDurationEstimation = new HashMap<Integer, Long>();
	Config config = Config.getInstance();
	Path inputFolder;

	Map<String, Long> trainSetDurations = new HashMap<String, Long>();
	Map<String, Double> trainSetSizes = new HashMap<String, Double>();

	static final Logger logger = LoggerFactory
			.getLogger(ApplicationEstimator.class);

	public ApplicationEstimator() {
		inputFolder = Paths.get(config.benchmarkFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}
	}

	public void estimateDuration() throws IOException {

		DirectoryStream<Path> directoryStream = Files
				.newDirectoryStream(inputFolder);

		for (Path benchmarkfolder : directoryStream) {
			if (benchmarkfolder.toFile().isDirectory()) {
				logger.debug("loading benchmark from folder"
						+ benchmarkfolder.getFileName());
				Path infoFile = Paths.get(benchmarkfolder.toAbsolutePath()
						.toString(), "application.info");
				double size = getSizeFromInfoFile(infoFile);
				String appId = getAppIDFromInfoFile(infoFile);
				
				trainSetSizes.put(appId, size);
				
				
				Path applicationEvents = Paths.get(benchmarkfolder.toAbsolutePath()
						.toString(), "application.csv");
				
				Reader eventsReader = new FileReader(applicationEvents.toFile());
				Iterable<CSVRecord> eventsRecords = CSVFormat.EXCEL.withHeader().parse(eventsReader);				
				long start=0;
				long end=0;
				for (CSVRecord record : eventsRecords) {
					String event = record.get("Event");
					long timestamp = Long.decode(record.get("Timestamp"));
					if(event.equals("SparkListenerApplicationStart")){
						start = timestamp;
					}else if (event.equals("SparkListenerApplicationEnd")){
						end = timestamp;
					}
				}
				trainSetDurations.put(appId, end-start);
				eventsReader.close();
			}
		
		
		//TODO: parse stages, dags..
		
		}

	}

	private double getSizeFromInfoFile(Path infoFile) throws IOException {
	
		for(String line: Files.readAllLines(infoFile, Charset.defaultCharset()))
		 if(line.contains("Data Size"))
			 return Double.parseDouble(line.split(";")[1]);
	 	 
		return 0;
	}

	private String getAppIDFromInfoFile(Path infoFile) throws IOException {
		for(String line: Files.readAllLines(infoFile, Charset.defaultCharset()))
			 if(line.contains("Application Id"))
				 return line.split(";")[1];
		return null;
	}
}
