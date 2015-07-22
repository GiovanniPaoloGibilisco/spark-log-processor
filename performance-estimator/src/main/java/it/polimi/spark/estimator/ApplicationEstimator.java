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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
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

	private static final int maxDegree = 3;

	Map<String, Long> durations = new HashMap<String, Long>();
	Map<String, Double> sizes = new HashMap<String, Double>();
	Map<Integer, Map<String, Long>> stageDurations = new HashMap<>();
	Map<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>> jobDags = new HashMap<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>>();
	Set<String> trainSet = new HashSet<>();
	Set<String> crossValidationSet = new HashSet<>();
	Set<String> testSet = new HashSet<>();

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
		// showData();

		// extract testing, training and cross validation sets (testing sets is
		// biased toward biggest applications)
		testSet = extractTestSet();
		trainSet = extractTrainSet();
		crossValidationSet = extractCVSet();

		logger.info("Training Set");
		showSet(trainSet);
		logger.info("Testing Set");
		showSet(testSet);
		logger.info("Cross Validation Set");
		showSet(crossValidationSet);

		// estimate the durations and output the results to a csv file
		OutputStream stageOs = new FileOutputStream(Paths.get(
				config.outputFile, "StageEstimations.csv").toString());
		BufferedWriter stageBr = new BufferedWriter(new OutputStreamWriter(
				stageOs, "UTF-8"));
		// the schema first
		stageBr.write("Stage ID,Real Duration,Estimated Duration,Error,Error Percentage,Size,Error/AppDuration");
		stageBr.write("\n");

		// stave the metrics of the estimation of the applications in a csv file
		OutputStream apsOs = new FileOutputStream(Paths.get(config.outputFile,
				"AppsEstimations.csv").toString());
		BufferedWriter appsBr = new BufferedWriter(new OutputStreamWriter(
				apsOs, "UTF-8"));
		// the schema first
		appsBr.write("App ID,Real Duration,Estimated Duration,Error,Error Percentage,Size");
		appsBr.write("\n");

		// for each application, estimate its duration
		for (String appId : testSet) {
			double appSize = sizes.get(appId);
			logger.debug("Estimating stage durations for app " + appId
					+ " size: " + appSize);
			Map<Integer, Long> estimatedStageDurations = estimateStageDurations(appSize);

			// Map<Integer, Long> estimatedStageDurations =
			// estimateStageDurations(appSize,2);

			for (int stageId : estimatedStageDurations.keySet()) {
				long realDuration = stageDurations.get(stageId).get(appId);
				long estimatedDuration = estimatedStageDurations.get(stageId);

				logger.trace("Stage: " + stageId + " Estimated Duration: "
						+ estimatedDuration + " Real Duration: " + realDuration
						+ " Error: "
						+ Math.abs(estimatedDuration - realDuration)
						+ " Error Percentage: "
						+ Math.abs(estimatedDuration - realDuration)
						/ (double) realDuration + " Stage error/App duration"
						+ Math.abs(estimatedDuration - realDuration)
						/ durations.get(appId));

				stageBr.write(stageId
						+ ","
						+ realDuration
						+ ","
						+ estimatedDuration
						+ ","
						+ Math.abs(estimatedDuration - realDuration)
						+ ","
						+ (Math.abs(estimatedDuration - realDuration) / (double) realDuration)
						+ "," + appSize + ","
						+ (double) Math.abs(estimatedDuration - realDuration)
						/ durations.get(appId));
				stageBr.write("\n");
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
			appsBr.write(appId + "," + durations.get(appId) + "," + duration
					+ "," + Math.abs(duration - durations.get(appId)) + ","
					+ (Math.abs(duration - durations.get(appId)))
					/ (double) durations.get(appId) + "," + sizes.get(appId));
			appsBr.write("\n");
		}

		appsBr.flush();
		appsBr.close();
		stageBr.flush();
		stageBr.close();
	}

	/**
	 * Estimates the stage duration using a polynomial function of the specified
	 * degree
	 * 
	 * @param newSize
	 * @param degree
	 * @return
	 */
	@SuppressWarnings("unused")
	private Map<Integer, Long> estimateStageDurations(double newSize, int degree) {
		Map<Integer, Long> estimates = new HashMap<Integer, Long>();

		for (int stageId : stageDurations.keySet()) {
			List<Double> trainSizes = new ArrayList<Double>();
			List<Long> trainDurations = new ArrayList<Long>();
			for (String appId : trainSet) {
				trainSizes.add(sizes.get(appId));
				trainDurations.add(stageDurations.get(stageId).get(appId));
			}
			estimates.put(
					stageId,
					estimateStageDuration(trainSizes, trainDurations, newSize,
							degree));
		}
		return estimates;
	}

	/**
	 * Estimated the stage duration of an application according to its data
	 * size. This function uses the training and cross validation sets to select
	 * the best polynomial model and train it.
	 * 
	 * @param newSize
	 * @return
	 */
	private Map<Integer, Long> estimateStageDurations(double newSize) {
		Map<Integer, Long> estimates = new HashMap<Integer, Long>();

		// Select and train a model for each stage
		for (int stageId : stageDurations.keySet()) {
			List<Double> trainSizes = new ArrayList<Double>();
			List<Long> trainDurations = new ArrayList<Long>();
			List<Double> cvSizes = new ArrayList<Double>();
			List<Long> cvDurations = new ArrayList<Long>();
			for (String appId : trainSet) {
				trainSizes.add(sizes.get(appId));
				trainDurations.add(stageDurations.get(stageId).get(appId));
			}
			for (String appId : crossValidationSet) {
				cvSizes.add(sizes.get(appId));
				cvDurations.add(stageDurations.get(stageId).get(appId));
			}

			// train polynomial functions of orders up to maxDegree using the
			// trainSet
			List<PolynomialFunction> functions = new ArrayList<PolynomialFunction>(
					maxDegree);
			for (int i = 1; i <= maxDegree; i++) {
				functions.add(getStageEstimationFunction(trainSizes,
						trainDurations, i));
			}

			// test how well each function generalizes using the cross
			// validation set
			// the cross validation error is defined as: (1/2m) *
			// sum{1..m}(h(x_i)-y_i)^2
			// m = size of the cross validation set
			// h = the polynomial function we are evaluating (hypothesis)
			// x_i = the size of the data for the i-th stage in the cv set
			// y_i = the duration of the i-th stage in the cv set
			List<Double> cvErrors = new ArrayList<>(maxDegree);
			int m = cvSizes.size();
			for (int j = 0; j < functions.size(); j++) {
				double cvError = 0;
				for (int i = 0; i < m; i++) {
					cvError = Math
							.pow((functions.get(j).value(cvSizes.get(i)) - cvDurations
									.get(i).doubleValue()), 2);
				}
				cvError *= 1.0 / (2.0 * m);
				cvErrors.add(cvError);
				logger.debug("Polinomial Degree: " + (j + 1)
						+ " Cross Validation Error: " + cvError);
			}

			// Select the model with the lowest cross validation error (better
			// generalizes)
			int bestDegree = cvErrors.indexOf(Collections.min(cvErrors));
			logger.debug("Best degree for stage " + stageId + " is "
					+ (bestDegree + 1));

			// Now that the degree for sta stage is fixed we can re-train that
			// model using also the cv set
			trainSizes.addAll(cvSizes);
			trainDurations.addAll(cvDurations);
			PolynomialFunction bestModel = getStageEstimationFunction(
					trainSizes, trainDurations, bestDegree+1);

			// finally we use it to estimate the new value
			long estimation = new Double(bestModel.value(newSize)).longValue();

			estimates.put(stageId, estimation);
		}
		return estimates;
	}

	private void showSet(Set<String> set) {
		for (String id : set)
			logger.info("App: " + id + " Size: " + sizes.get(id));
	}

	private Set<String> extractTrainSet() {

		int trainSetSize = new Double(sizes.size() * config.trainFraction)
				.intValue();

		List<String> availableApps = new ArrayList<>(sizes.keySet());
		availableApps.removeAll(testSet);

		Collections.shuffle(availableApps);

		return new HashSet<>(availableApps.subList(0, trainSetSize));
	}

	private Set<String> extractTestSet() {

		Set<String> testSet = new HashSet<String>();
		SortedMap<Double, String> size2application = new TreeMap<>();
		for (String id : sizes.keySet())
			size2application.put(sizes.get(id), id);

		int testSetSize = new Double(sizes.size()
				* (1 - config.trainFraction - config.crossValidationFraction))
				.intValue();

		testSetSize = testSetSize < 1 ? 1 : testSetSize;

		List<Double> sortedSizes = new ArrayList<Double>(
				size2application.keySet());

		for (double size : sortedSizes.subList(sizes.size() - testSetSize,
				sizes.size()))
			testSet.add(size2application.get(size));
		return testSet;
	}

	private Set<String> extractCVSet() {
		List<String> availableApps = new ArrayList<>(sizes.keySet());
		availableApps.removeAll(testSet);
		availableApps.removeAll(trainSet);
		return new HashSet<>(availableApps);

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
			//skip non executed stages
			if (duration == 0)
				continue;
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

	public static PolynomialFunction getStageEstimationFunction(
			List<Double> trainingSizes, List<Long> trainingDurations, int degree) {

		PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);
		List<WeightedObservedPoint> points = new ArrayList<>();

		for (int i = 0; i < trainingSizes.size(); i++)
			points.add(new WeightedObservedPoint(1, trainingSizes.get(i),
					new Long(trainingDurations.get(i)).doubleValue()));

		return new PolynomialFunction(fitter.fit(points));

	}

	public static long estimateStageDuration(List<Double> trainingSizes,
			List<Long> trainingDurations, double newSize, int degree) {

		PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);
		List<WeightedObservedPoint> points = new ArrayList<>();

		for (int i = 0; i < trainingSizes.size(); i++)
			points.add(new WeightedObservedPoint(1, trainingSizes.get(i),
					new Long(trainingDurations.get(i)).doubleValue()));

		PolynomialFunction function = new PolynomialFunction(fitter.fit(points));

		return new Double(function.value(newSize)).longValue();

	}
}
