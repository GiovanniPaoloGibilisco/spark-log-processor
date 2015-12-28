package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Estimate the duration of an application starting from the real stage
 * durations and aggregating using the estimateJobduration utility function (in
 * Utils.java)
 * 
 * @author giovanni
 *
 */
public class AggregationEstimator {

	long applicationDurationEstimation = 0;
	Map<Integer, Long> jobDurationEstimation = new HashMap<Integer, Long>();
	Map<Integer, Long> jobdurationEstimationError = new HashMap<Integer, Long>();
	Map<Integer, Long> jobDuration = new HashMap<Integer, Long>();
	Config config = Config.getInstance();
	Path inputFolder;
	Map<String, DirectedAcyclicGraph<Stagenode, DefaultEdge>> stageDags = new LinkedHashMap<String, DirectedAcyclicGraph<Stagenode, DefaultEdge>>();
	static final Logger logger = LoggerFactory
			.getLogger(AggregationEstimator.class);

	public AggregationEstimator() {

		inputFolder = Paths.get(config.dagInputFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}
	}

	public  List<EstimationResult> estimateDuration() throws ClassNotFoundException, IOException,
			SQLException {
		// load the dags

		ArrayList<EstimationResult> results = new ArrayList<>();
		if (inputFolder.toFile().isFile()) {
			logger.info("Input folder is actually a file, processing only that file");
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = Utils
					.deserializeFile(inputFolder);
			if (dag != null)
				stageDags.put(inputFolder.getFileName().toString(), dag);
		} else {
			DirectoryStream<Path> directoryStream = Files
					.newDirectoryStream(inputFolder);
			for (Path file : directoryStream) {
				if (file.toFile().isFile()) {
					// should check first that this is the correct type of Dag,
					// or
					// delegate it to the deserialization function
					logger.debug("loading " + file.getFileName() + " dag");
					DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = Utils
							.deserializeFile(file);
					if (dag != null)
						stageDags.put(file.getFileName().toString(), dag);
				}
			}
		}

		// show some infos
		logger.info("Loaded " + stageDags.size() + " dags");
		for (String dagName : stageDags.keySet()) {
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = stageDags
					.get(dagName);
			logger.info("Dag " + dagName + " has: " + dag.vertexSet().size()
					+ " vertexes and " + dag.edgeSet().size() + " edges");
		}

		// load stage performance info
		Path stageInfoFile = Paths.get(config.stagePerformanceFile);
		if (!stageInfoFile.toFile().exists()) {
			logger.error("Stage info file" + stageInfoFile + " does not exist");
			return results;
		}
		logger.info("loading Stage performance info from "
				+ stageInfoFile.getFileName());

		Reader stageReader = new FileReader(stageInfoFile.toFile());
		Iterable<CSVRecord> stageRecords = CSVFormat.EXCEL.withHeader().parse(
				stageReader);
		Map<Integer, Long> stageDurationInfo = new HashMap<Integer, Long>();
		for (CSVRecord record : stageRecords) {
			int stageId = Integer.decode(record.get("Stage ID"));
			long duration = 0;
			// if the stage has not been executed its duration is 0
			if (Boolean.parseBoolean(record.get("Executed"))) {
				duration = Long.decode(record.get("Duration"));
			}
			stageDurationInfo.put(stageId, duration);
		}
		stageReader.close();

		// Load job performance info
		Path jobInfoFile = Paths.get(config.jobPerformanceFile);
		if (!jobInfoFile.toFile().exists()) {
			logger.error("Job Info File" + jobInfoFile + " does not exist");
			return results;
		}
		logger.info("loading Job performance info from "
				+ jobInfoFile.getFileName());
		Reader jobReader = new FileReader(jobInfoFile.toFile());
		Iterable<CSVRecord> jobRecords = CSVFormat.EXCEL.withHeader().parse(
				jobReader);
		for (CSVRecord jobRecord : jobRecords) {
			int jobId = Integer.decode(jobRecord.get("Job ID"));
			long duration = Long.decode(jobRecord.get("Duration"));
			jobDuration.put(jobId, duration);
		}

		// estimate Job durations from Stage Durations
		for (String dagName : stageDags.keySet()) {
			int jobId = Integer.decode(dagName.split("Job_")[1]);
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = stageDags
					.get(dagName);

			long estimatedDuration = Utils.estimateJobDuration(dag,
					stageDurationInfo);
			long actualDuration = jobDuration.get(jobId);
			long error = Math.abs(estimatedDuration - actualDuration);

			jobDurationEstimation.put(jobId, estimatedDuration);
			jobdurationEstimationError.put(jobId, error);

			float errorPercentage = ((float) error / (float) actualDuration) * 100;

			logger.info("Job " + jobId + ": expected duration: "
					+ estimatedDuration + " ms. actual duration "
					+ actualDuration + " ms. error: " + error
					+ " error percentage (error/actual): " + errorPercentage
					+ "%");

			// sum up application execution time
			applicationDurationEstimation += estimatedDuration;
		}

		boolean output = true;
		if (config.outputFile == null) {
			logger.warn("An output file has not been specified or can not be created.");
			output = false;
		}

		// if specified upload the result to the DB
		DBHandler dbHandler = null;
		if (config.toDB) {
			if (config.dbUser == null)
				logger.warn("No user name has been specified for the connection with the DB, results will not be uploaded");
			if (config.dbPassword == null)
				logger.warn("No password has been specified for the connection with the DB, results will not be uploaded");
			if (config.clusterName == null)
				logger.warn("No cluster name has been specified, a cluster name is needed to add applications to the DB");
			if (config.dbPassword == null)
				logger.warn("No appId has been specified, a cluster name is needed to add applications to the DB");
			if (config.dbUser != null && config.dbPassword != null
					&& config.clusterName != null && config.appId != null) {
				dbHandler = new DBHandler(config.dbUrl, config.dbUser,
						config.dbPassword);
				logger.info("Saving results to the DB");

				dbHandler.updateApplicationExpectedExecutionTime(
						config.clusterName, config.appId,
						(double) applicationDurationEstimation);

				for (int jobId : jobDurationEstimation.keySet())
					dbHandler.updateJobExpectedExecutionTime(
							config.clusterName, config.appId, jobId,
							jobDurationEstimation.get(jobId));
				
			}
		}

		// save the output to file
		CSVPrinter csvFilePrinter = null;
		FileWriter fileWriter = null;
		if (output) {
			fileWriter = new FileWriter(Paths.get(config.outputFile).toFile());
			CSVFormat format = CSVFormat.DEFAULT.withHeader("Job ID",
					"Estimated Duration", "Actual Duration", "Error",
					"Error Percentage");
			csvFilePrinter = new CSVPrinter(fileWriter, format);

			// export and print the output
			for (int jobId : jobDuration.keySet()) {
				csvFilePrinter
						.printRecord(
								jobId,
								jobDurationEstimation.get(jobId),
								jobDuration.get(jobId),
								jobdurationEstimationError.get(jobId),
								((float) jobdurationEstimationError.get(jobId) / (float) jobDuration
										.get(jobId)) * 100);
			}

			fileWriter.flush();
			fileWriter.close();
			csvFilePrinter.close();
		}
		logger.info("Total Estimated Application execution time: "
				+ applicationDurationEstimation + " ms.");
		
		results.add(new EstimationResult(null, -1, -1, applicationDurationEstimation));
		return results;
	}
}
