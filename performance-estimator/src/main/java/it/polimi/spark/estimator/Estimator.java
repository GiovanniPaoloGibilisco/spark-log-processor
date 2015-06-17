package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StreamCorruptedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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

public class Estimator {

	private static Config config;
	static final Logger logger = LoggerFactory.getLogger(Estimator.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		Config.init(args);
		config = Config.getInstance();

		if (config.usage) {
			config.usage();
			return;
		}
		Path inputFolder = Paths.get(config.dagInputFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}

		// load the dags
		Map<String, DirectedAcyclicGraph<Stagenode, DefaultEdge>> stageDags = new LinkedHashMap<String, DirectedAcyclicGraph<Stagenode, DefaultEdge>>();
		if (inputFolder.toFile().isFile()) {
			logger.info("Input folder is actually a file, processing only that file");
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = deserializeFile(inputFolder);
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
					DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = deserializeFile(file);
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

		// load the performance file
		Path performanceFile = Paths.get(config.stagePerformanceFile);
		if (!performanceFile.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}
		logger.info("loading performance info from "
				+ performanceFile.getFileName());
		Reader in = new FileReader(performanceFile.toFile());
		Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
		Map<Integer, CSVRecord> stagePerformanceInfo = new LinkedHashMap<Integer, CSVRecord>();
		Map<Integer, Long> stageDurationInfo = new HashMap<Integer, Long>();
		for (CSVRecord record : records) {
			int stageId = Integer.decode(record.get("Stage ID"));
			stagePerformanceInfo.put(stageId, record);

			long duration = 0;
			// if the stage has not been executed its duration is 0
			if (Boolean.parseBoolean(record.get("computed"))) {
				long stageSubmissionTime = Long.decode(record
						.get("Submission Time"));
				long stageCompletionTime = Long.decode(record
						.get("Completion Time"));
				duration = stageCompletionTime - stageSubmissionTime;
			}
			stageDurationInfo.put(stageId, duration);
		}

		// calculate the job execution time by completion - submission time
		Map<Integer, Long> jobCompletionTimes = new HashMap<Integer, Long>();
		for (CSVRecord stageRecord : stagePerformanceInfo.values()) {
			int jobId = Integer.decode(stageRecord.get("Job ID"));
			boolean computed = Boolean
					.parseBoolean(stageRecord.get("computed"));
			if (computed && !jobCompletionTimes.containsKey(jobId)) {
				long submissionTime = Long.decode(stageRecord
						.get("JobSubmissionTime"));
				long completionTime = Long.decode(stageRecord
						.get("JobCompletionTime"));
				jobCompletionTimes.put(jobId, completionTime - submissionTime);
			}
		}

		long estimatedApplicationExecutionTime = 0;
		boolean output = true;
		if (config.outputFile == null) {
			logger.info("An output file has not been specified or can not be created.");
			output = false;
		}
		CSVPrinter csvFilePrinter = null;
		FileWriter fileWriter = null;
		if (output) {
			fileWriter = new FileWriter(Paths.get(config.outputFile).toFile());
			CSVFormat format = CSVFormat.DEFAULT.withHeader("Job ID",
					"Estimated Duration", "Actual Duration", "Error",
					"Error Percentage");
			csvFilePrinter = new CSVPrinter(fileWriter, format);
		}

		for (String dagName : stageDags.keySet()) {
			int jobId = Integer.decode(dagName.split("Job_")[1]);
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = stageDags
					.get(dagName);
			Stagenode finalStage = null;
			for (Stagenode stage : dag.vertexSet()) {
				if (dag.outDegreeOf(stage) == 0) {
					finalStage = stage;
					break;
				}
			}

			long estimatedDuration = estimateJobDuration(dag,
					stageDurationInfo, finalStage);
			long actualDuration = jobCompletionTimes.get(jobId);
			long error = Math.abs(estimatedDuration - actualDuration);
			float errorPercentage = ((float) error / (float) actualDuration) * 100;

			// export and print the output
			if (output) {
				csvFilePrinter.printRecord(jobId, estimatedDuration,
						actualDuration, error, errorPercentage);
			}
			logger.info("Job " + jobId + ": expected duration: "
					+ estimatedDuration + " ms. actual duration "
					+ actualDuration + " ms. error: " + error
					+ " error percentage (error/actual): " + errorPercentage
					+ "%");
			estimatedApplicationExecutionTime += estimatedDuration;
		}
		if (output) {
			fileWriter.flush();
			fileWriter.close();
			csvFilePrinter.close();
		}
		logger.info("Total Estimated Application execution time: "
				+ estimatedApplicationExecutionTime + " ms.");

	}

	/**
	 * Deserializes the file containign a Stage DAG
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static DirectedAcyclicGraph<Stagenode, DefaultEdge> deserializeFile(
			Path file) throws IOException, ClassNotFoundException {
		InputStream fileIn = Files.newInputStream(file);
		DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = null;
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(fileIn);
			dag = (DirectedAcyclicGraph<Stagenode, DefaultEdge>) in.readObject();
		} catch (StreamCorruptedException e) {
			logger.warn("file "
					+ file.getFileName()
					+ " is not a valid DAG or has been serialized badly. Skipping it");

		} finally {
			if (in != null)
				in.close();
			fileIn.close();
		}

		return dag;
	}

	/**
	 * Gets the duration of the dag starting from the finalStage and using:
	 * "duration(finalStage) + max(duration(finalStage.parents))" it operates
	 * recursively on the entire DAG.
	 * 
	 * @param dag
	 * @param stageDuration
	 * @param finalStage
	 * @return
	 */
	private static long estimateJobDuration(
			// TODO: find a smarter way to do this by saving partial
			// computations, perform branch pruning or some other tricks.
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag,
			Map<Integer, Long> stageDuration, Stagenode finalStage) {

		// default case, if the stage does not depend on other stages then it is
		// just its duration
		if (dag.inDegreeOf(finalStage) == 0)
			return stageDuration.get(finalStage.getId());

		// if the stage has dependencies the duration is is own duration plus
		// the maximum duration of its parents.
		List<Long> parentDurations = new ArrayList<Long>();
		for (DefaultEdge edge : dag.incomingEdgesOf(finalStage))
			parentDurations.add(estimateJobDuration(dag, stageDuration,
					dag.getEdgeSource(edge)));

		return stageDuration.get(finalStage.getId())
				+ Collections.max(parentDurations);
	}
}
