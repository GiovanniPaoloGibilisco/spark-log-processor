package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
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

	Map<String, Long> durations = new HashMap<String, Long>();
	Map<String, Double> sizes = new HashMap<String, Double>();
	Map<Integer, Map<String, Long>> stageDurations = new HashMap<>();
	Map<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>> jobDags = new HashMap<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>>();
	Set<String> trainingApplications = new HashSet<>();
	Set<String> testingApplications = new HashSet<>();

	static final Logger logger = LoggerFactory
			.getLogger(ApplicationEstimator.class);

	public ApplicationEstimator() {
		inputFolder = Paths.get(config.benchmarkFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}
	}

	public void estimateDuration() throws IOException, ClassNotFoundException {

		DirectoryStream<Path> directoryStream = Files
				.newDirectoryStream(inputFolder);

		// load all the available data
		for (Path benchmarkfolder : directoryStream) {
			if (benchmarkfolder.toFile().isDirectory()) {
				logger.trace("loading benchmark from folder"
						+ benchmarkfolder.getFileName());
				Path infoFile = Paths.get(benchmarkfolder.toAbsolutePath()
						.toString(), "application.info");
				String appId = getAppIDFromInfoFile(infoFile);

				getApplicationSize(benchmarkfolder, appId);

				getApplicationDuration(benchmarkfolder, appId);

				getStagesDuration(benchmarkfolder, appId);

				getJobDags(benchmarkfolder);

			}
		}

		if (sizes.size() < 3) {
			logger.info("At least 3 applications are needed");
			return;
		}

		directoryStream.close();
		showData();

		extractTrainingAndTestingSet();

		showTrainingSet();
		showTestingSet();

		// estimate the durations and otuput the results to a csv file
		OutputStream os = new FileOutputStream(config.outputFile);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		// the schema first
		br.write("Stage ID,Real Duration,Estimated Duration,Error,Error Percentage,Size");
		br.write("\n");

		for (String appId : testingApplications) {
			double appSize = sizes.get(appId);
			logger.info("Estimating stage durations for app " + appId
					+ " size: " + appSize);
			Map<Integer, Long> estimatedStageDurations = estimateStageDurations(appSize);

			for (int stageId : estimatedStageDurations.keySet()) {
				long realDuration = stageDurations.get(stageId).get(appId);
				long estimatedDuration = estimatedStageDurations.get(stageId);

				logger.trace("Stage: " + stageId + " Estimated Duration: "
						+ estimatedDuration + " Real Duration: " + realDuration
						+ " Error: "
						+ Math.abs(estimatedDuration - realDuration)
						+ " Error Percentage: "
						+ Math.abs(estimatedDuration - realDuration)
						/ (double) realDuration);

				br.write(stageId
						+ ","
						+ realDuration
						+ ","
						+ estimatedDuration
						+ ","
						+ Math.abs(estimatedDuration - realDuration)
						+ ","
						+ (Math.abs(estimatedDuration - realDuration) / (double) realDuration)
						+ "," + appSize);
				br.write("\n");
			}

			long duration = 0;
			for (int jobId : jobDags.keySet())
				duration += Utils.estimateJobDuration(jobDags.get(jobId),
						estimatedStageDurations);

			logger.info("App: " + appId + " Estimated Duration: " + duration
					+ " Real Duration: " + durations.get(appId) + "Error: "
					+ Math.abs(duration - durations.get(appId))
					+ " Error Percantage "
					+ (Math.abs(duration - durations.get(appId)))
					/ (double) durations.get(appId));
		}

		br.flush();
		br.close();
	}

	private Map<Integer, Long> estimateStageDurations(double newSize) {
		Map<Integer, Long> estimates = new HashMap<Integer, Long>();

		for (int stageId : stageDurations.keySet()) {
			List<Double> trainSizes = new ArrayList<Double>();
			List<Long> trainDurations = new ArrayList<Long>();
			for (String appId : trainingApplications) {
				trainSizes.add(sizes.get(appId));
				trainDurations.add(stageDurations.get(stageId).get(appId));
			}
			estimates.put(stageId,
					estimateStageDuration(trainSizes, trainDurations, newSize));
		}
		return estimates;
	}

	private void showTestingSet() {
		logger.info("Testing Set:");
		for (String id : testingApplications)
			logger.info("App: " + id + " Size: " + sizes.get(id));
	}

	private void showTrainingSet() {
		logger.info("Training Set:");
		for (String id : trainingApplications)
			logger.info("App: " + id + " Size: " + sizes.get(id));
	}

	private void extractTrainingAndTestingSet() {

		SortedMap<Double, String> size2application = new TreeMap<>();
		for (String id : sizes.keySet())
			size2application.put(sizes.get(id), id);

		int trainSetSize = new Double(sizes.size() * config.trainFraction)
				.intValue();
		// make sure we have at least two points in the training set
		trainSetSize = trainSetSize < 2 ? 2 : trainSetSize;

		List<Double> sortedSizes = new ArrayList<Double>(
				size2application.keySet());

		for (double size : sortedSizes.subList(0, trainSetSize))
			trainingApplications.add(size2application.get(size));

		for (double size : sortedSizes.subList(trainSetSize, sizes.size()))
			testingApplications.add(size2application.get(size));

	}

	private void getJobDags(Path benchmarkfolder) throws IOException,
			ClassNotFoundException {
		// Assuming dag of all jobs are the same, we have to do it
		// only for the first folder
		if (jobDags.size() == 0) {
			Path dagDirectory = Paths.get(benchmarkfolder.toAbsolutePath()
					.toString(), "dags");

			DirectoryStream<Path> dagStream = Files
					.newDirectoryStream(dagDirectory);
			for (Path file : dagStream) {
				if (file.toFile().isFile()) {
					logger.debug("loading " + file.getFileName() + " dag");
					DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = Utils
							.deserializeFile(file);
					if (dag != null) {
						int jobId = dag.vertexSet().iterator().next()
								.getJobId();
						jobDags.put(jobId, dag);
					}
				}
			}
			dagStream.close();
		}
	}

	private void getStagesDuration(Path benchmarkfolder, String appId)
			throws FileNotFoundException, IOException {
		// load the duration of all stages
		Path applicationStages = Paths.get(benchmarkfolder.toAbsolutePath()
				.toString(), "StageDetails.csv");
		Reader eventsReader;
		Iterable<CSVRecord> eventsRecords;
		eventsReader = new FileReader(applicationStages.toFile());
		eventsRecords = CSVFormat.EXCEL.withHeader().parse(eventsReader);
		for (CSVRecord record : eventsRecords) {
			int stageID = Integer.decode(record.get("Stage ID"));
			long duration = Long.decode(record.get("Duration"));
			if (!stageDurations.containsKey(stageID))
				stageDurations.put(stageID, new HashMap<String, Long>());
			stageDurations.get(stageID).put(appId, duration);
		}
		eventsReader.close();
	}

	private void getApplicationDuration(Path benchmarkfolder, String appId)
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
		durations.put(appId, end - start);
		eventsReader.close();
	}

	private void getApplicationSize(Path benchmarkfolder, String appId)
			throws IOException {
		// Load the size of the application and its id
		Path infoFile = Paths.get(benchmarkfolder.toAbsolutePath().toString(),
				"application.info");
		double size = getSizeFromInfoFile(infoFile);

		sizes.put(appId, size);
	}

	private void showData() {
		for (String id : sizes.keySet()) {
			logger.info("App: " + id + " size: " + sizes.get(id)
					+ " durations: " + durations.get(id));
		}

		for (int id : stageDurations.keySet()) {
			String durations = "";
			for (String app : stageDurations.get(id).keySet())
				durations += stageDurations.get(id).get(app) + ",";
			logger.info("Stage: " + id + " durations: " + durations);
		}

		for (int jobId : jobDags.keySet()) {
			logger.info("Job " + jobId + " has "
					+ jobDags.get(jobId).vertexSet().size()
					+ " stages in the DAG");
		}
	}

	private double getSizeFromInfoFile(Path infoFile) throws IOException {

		for (String line : Files.readAllLines(infoFile,
				Charset.defaultCharset()))
			if (line.contains("Data Size"))
				return Double.parseDouble(line.split(";")[1]);

		return 0;
	}

	private String getAppIDFromInfoFile(Path infoFile) throws IOException {
		for (String line : Files.readAllLines(infoFile,
				Charset.defaultCharset()))
			if (line.contains("Application Id"))
				return line.split(";")[1];
		return null;
	}

	public static long estimateStageDuration(List<Double> trainingSizes,
			List<Long> trainingDurations, double newSize) {

		double[][] data;
		SimpleRegression regression = new SimpleRegression(true);
		for (int i = 0; i < trainingSizes.size(); i++)
			regression.addData(trainingSizes.get(i),
					new Long(trainingDurations.get(i)).doubleValue());

		logger.trace("Model trained with " + trainingSizes.size()
				+ " data points. Model: y=" + regression.getSlope() + "*x + "
				+ regression.getIntercept());
		logger.trace("Mean Square Error: " + regression.getMeanSquareError()
				+ " RSquare: " + regression.getRSquare());

		return new Double(regression.predict(newSize)).longValue();
	}
}
