package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StreamCorruptedException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	static final Logger logger = LoggerFactory.getLogger(Utils.class);

	/**
	 * Deserializes the file containign a Stage DAG
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	static DirectedAcyclicGraph<Stagenode, DefaultEdge> deserializeFile(
			Path file) throws IOException, ClassNotFoundException {
		InputStream fileIn = Files.newInputStream(file);
		DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = null;
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(fileIn);
			dag = (DirectedAcyclicGraph<Stagenode, DefaultEdge>) in
					.readObject();
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
			if (stageDuration.containsKey(finalStage.getId()))
				return stageDuration.get(finalStage.getId());
			else
				return 0;

		// if the stage has dependencies the duration is is own duration plus
		// the maximum duration of its parents.
		List<Long> parentDurations = new ArrayList<Long>();
		for (DefaultEdge edge : dag.incomingEdgesOf(finalStage))
			parentDurations.add(estimateJobDuration(dag, stageDuration,
					dag.getEdgeSource(edge)));

		// use zero as default if the final stage has not been executed
		long currentstageduration = 0;
		if (stageDuration.containsKey(finalStage.getId()))
			currentstageduration = stageDuration.get(finalStage.getId());
		return currentstageduration + Collections.max(parentDurations);

	}

	/**
	 * Gets the duration of the dag starting from the finalStage and using:
	 * "duration(finalStage) + max(duration(finalStage.parents))" it operates
	 * recursively on the entire DAG.
	 * 
	 * @param dag
	 * @param stageDuration
	 * @return
	 */
	public static long estimateJobDuration(
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag,
			Map<Integer, Long> stageDuration) {
		Stagenode finalStage = null;
		for (Stagenode stage : dag.vertexSet()) {
			if (dag.outDegreeOf(stage) == 0) {
				finalStage = stage;
				break;
			}
		}
		return estimateJobDuration(dag, stageDuration, finalStage);
	}

	/**
	 * Export a string with the structure of the maximimation formula
	 * 
	 * @param dag
	 * @param stageDuration
	 * @return
	 */
	static String exportJobDurationFunction(
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag) {
		Stagenode finalStage = null;
		for (Stagenode stage : dag.vertexSet()) {
			if (dag.outDegreeOf(stage) == 0) {
				finalStage = stage;
				break;
			}
		}
		
		return exportJobDurationFunction(dag, finalStage);
	}

	private static String exportJobDurationFunction(
			// TODO: find a smarter way to do this by saving partial
			// computations, perform branch pruning or some other tricks.
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag,
			Stagenode finalStage) {

		// default case, if the stage does not depend on other stages then it is
		// just its duration
		if (dag.inDegreeOf(finalStage) == 0)
			return "_" + Integer.toString(finalStage.getId()) + "_";
		// if it only has ancestor then no maximization is needed, just sum it
		// up

		int executedParents = 0;
		for (DefaultEdge edge : dag.incomingEdgesOf(finalStage))
			if (dag.getEdgeSource(edge).isExecuted())
				executedParents++;

		if (executedParents == 1) {
			for (DefaultEdge edge : dag.incomingEdgesOf(finalStage)) {
				Stagenode previousStage = dag.getEdgeSource(edge);
				if (previousStage.isExecuted())
					return "_" + Integer.toString(finalStage.getId()) + "_"
							+ " + "
							+ exportJobDurationFunction(dag, previousStage);
			}
		} else if (executedParents > 1) {

			// if the stage has 2 or more dependencies the duration is is own
			// duration plus
			// the maximum duration of its parents.
			String function = "_" + Integer.toString(finalStage.getId()) + "_"
					+ "+ max ( ";
			int maximizations = 1;
			// add the functions generated by the parents separated by a comma
			for (DefaultEdge edge : dag.incomingEdgesOf(finalStage)) {
				Stagenode previousStage = dag.getEdgeSource(edge);
				if (previousStage.isExecuted()) {
					if (executedParents > 2) {

						maximizations++;
						function += exportJobDurationFunction(dag,
								dag.getEdgeSource(edge))
								+ " , max ( ";
					} else {

						function += exportJobDurationFunction(dag,
								dag.getEdgeSource(edge))
								+ " , ";
					}
					executedParents--;
				}
			}
			// remove the last comma
			if (function.trim().endsWith(","))
				function = function.substring(0, function.lastIndexOf(","));
			for (int i = 0; i < maximizations; i++)
				function += " )";

			return function;
		}
		return "_" + Integer.toString(finalStage.getId()) + "_";
	}
	
	public static Map<Integer,Long> getStagesDuration(Path benchmarkfolder)
			throws FileNotFoundException, IOException {
		// load the duration of all stages
		HashMap<Integer, Long> durations = new HashMap<>();
		Path applicationStages = Paths.get(benchmarkfolder.toAbsolutePath()
				.toString(), "StageDetails.csv");
		Reader eventsReader;
		Iterable<CSVRecord> eventsRecords;
		eventsReader = new FileReader(applicationStages.toFile());
		eventsRecords = CSVFormat.EXCEL.withHeader().parse(eventsReader);
		for (CSVRecord record : eventsRecords) {
			int stageID = Integer.decode(record.get("Stage ID"));
			long duration = Long.decode(record.get("Duration"));
			// skip non executed stages
			if (duration == 0)
				continue;
			if (!durations.containsKey(stageID))
				durations.put(stageID,duration);			
		}
		eventsReader.close();
		return durations;
	}

	public static long getApplicationDuration(Path benchmarkfolder)
			throws FileNotFoundException, IOException {
		// load the duration of the application
		Path applicationEvents = Paths.get(benchmarkfolder.toAbsolutePath()
				.toString(), "application.csv");

		Reader eventsReader = new FileReader(applicationEvents.toFile());
		Iterable<CSVRecord> eventsRecords = CSVFormat.EXCEL.withHeader().parse(
				eventsReader);
		long start = 0;
		long end = 0;
		for (CSVRecord record : eventsRecords) {
			String event = record.get("Event");
			long timestamp = Long.decode(record.get("Timestamp"));
			if (event.equals("SparkListenerApplicationStart")) {
				start = timestamp;
			} else if (event.equals("SparkListenerApplicationEnd")) {
				end = timestamp;
			}
		}		
		eventsReader.close();
		return end - start;
	}

	public static double getApplicationSize(Path benchmarkfolder)
			throws IOException {
		// Load the size of the application and its id
		Path infoFile = Paths.get(benchmarkfolder.toAbsolutePath().toString(),
				"application.info");
		return getSizeFromInfoFile(infoFile);		
		
	}

	public static double getSizeFromInfoFile(Path infoFile) throws IOException {

		for (String line : Files.readAllLines(infoFile,
				Charset.defaultCharset()))
			if (line.contains("Data Size"))
				return Double.parseDouble(line.split(";")[1]);

		return 0;
	}

	public static String getAppIDFromInfoFile(Path infoFile) throws IOException {
		for (String line : Files.readAllLines(infoFile,
				Charset.defaultCharset()))
			if (line.contains("Application Id"))
				return line.split(";")[1];
		return null;
	}
}
