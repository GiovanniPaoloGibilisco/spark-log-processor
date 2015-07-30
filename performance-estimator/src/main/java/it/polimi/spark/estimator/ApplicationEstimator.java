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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
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
	private static final int k = 5;
	private static final int repetitions = 1000;

	Map<String, Long> durations = new HashMap<String, Long>();
	Map<String, Double> sizes = new HashMap<String, Double>();
	Map<Integer, Map<String, Long>> stageDurations = new HashMap<>();
	Map<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>> jobDags = new HashMap<Integer, DirectedAcyclicGraph<Stagenode, DefaultEdge>>();
	List<String> trainSet = new ArrayList<>();
	List<String> testSet = new ArrayList<>();
	Set<Integer> executedStages;

	static final Logger logger = LoggerFactory
			.getLogger(ApplicationEstimator.class);

	static final Logger cvLogger = LoggerFactory
			.getLogger("CrossValidationLogger");

	public ApplicationEstimator() {
		inputFolder = Paths.get(config.benchmarkFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}
	}

	public List<EstimationResult> estimateDuration() throws IOException,
			ClassNotFoundException {

		List<EstimationResult> results = new ArrayList<EstimationResult>();

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

		if (sizes.size() < 5) {
			logger.info("At least 5 applications are needed");
			return null;
		}

		directoryStream.close();
		// showData();

		// extract testing and training (testing sets is
		// biased toward biggest applications)
		testSet = extractTestSet();
		trainSet = extractTrainSet();

		logger.debug("Training Set");
		showSet(trainSet);
		logger.debug("Testing Set");
		showSet(testSet);

		FileUtils.forceMkdir(Paths.get(config.outputFile).toFile());

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

		// build stage models
		Map<Integer, PolynomialFunction> stageModels = new HashMap<>();

		// filter non executed stages
		executedStages = filterExecutedStages();

		for (int stageID : executedStages) {
			stageModels.put(stageID, buildStageModel(stageID));
		}

		// debugging, remove this later
		testSet.addAll(trainSet);
		//sort the tests in order of size for plotting convenience
		Collections.sort(testSet, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return sizes.get(o1).compareTo(sizes.get(o2));
			}
		});
		// for each application, estimate its duration
		for (String appId : testSet) {
			double appSize = sizes.get(appId);
			logger.debug("Estimating stage durations for app " + appId
					+ " size: " + appSize);
			Map<Integer, Long> estimatedStageDurations = new HashMap<Integer, Long>();
			for (int stageId : executedStages) {
				long estimatedDuration = new Double(stageModels.get(stageId)
						.value(appSize)).longValue();
				long realDuration = stageDurations.get(stageId).get(appId);
				estimatedStageDurations.put(stageId, estimatedDuration);

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

			long estimatedAppDuration = 0;
			long realDuration = durations.get(appId);
			for (int jobId : jobDags.keySet())
				estimatedAppDuration += Utils.estimateJobDuration(
						jobDags.get(jobId), estimatedStageDurations);
			EstimationResult estimation = new EstimationResult(appId, appSize,
					realDuration, estimatedAppDuration);
			results.add(estimation);

			logger.info("App: " + estimation.getAppID() + " Size: "
					+ estimation.getAppSize() + " Estimated Duration: "
					+ estimation.getEstimatedDuration() + " Real Duration: "
					+ estimation.getRealDuration() + " Error: "
					+ estimation.getError() + " Error Percantage "
					+ estimation.getRelativeError());
			appsBr.write(estimation.getAppID() + ","
					+ estimation.getRealDuration() + ","
					+ estimation.getEstimatedDuration() + ","
					+ estimation.getError() + ","
					+ estimation.getRelativeError() + ","
					+ estimation.getAppSize());
			appsBr.write("\n");

		}

		appsBr.flush();
		appsBr.close();
		stageBr.flush();
		stageBr.close();

		return results;
	}

	/**
	 * perform k-fold cross validation to build the model for the stage
	 * 
	 * @param stageID
	 * @return
	 */
	private PolynomialFunction buildStageModel(int stageID) {

		String log = "Stage"+stageID+",";
		List<String> appList = new ArrayList<String>(trainSet);
		int testBucketSize = Math.floorDiv(appList.size(), k);
		double[] testErrors = new double[maxDegree];
		Arrays.fill(testErrors, 0.0);
		
		int[] selections = new int[maxDegree];
		Arrays.fill(selections, 0);
		for (int rep = 0; rep < repetitions; rep++) {
			// Shuffle the train data
			Collections.shuffle(appList);
			for (int i = 0; i < k; i++) {
				int endIndex = (i + 1) * testBucketSize;
				// if the number of points is not a multiple of k then the last
				// testing bucket should be a little bigger
				if (i == k - 1)
					endIndex = appList.size();

				// generate the test bucket for fold i
				List<String> foldTestList = appList.subList(i * testBucketSize,
						endIndex);

				// generate the train bucket for fold i
				List<String> foldTrainList = new ArrayList<String>(appList);
				foldTrainList.removeAll(foldTestList);

				List<Double> trainSizes = new ArrayList<Double>();
				List<Long> trainDurations = new ArrayList<Long>();
				List<Double> testSizes = new ArrayList<Double>();
				List<Long> testDurations = new ArrayList<Long>();

				for (String appId : foldTrainList) {
					trainSizes.add(sizes.get(appId));
					trainDurations.add(stageDurations.get(stageID).get(appId));
				}
				for (String appId : foldTestList) {
					testSizes.add(sizes.get(appId));
					testDurations.add(stageDurations.get(stageID).get(appId));
				}

				// train polynomial functions of orders up to maxDegree using
				// the
				// trainSet
				List<PolynomialFunction> functions = new ArrayList<PolynomialFunction>(
						maxDegree);
				for (int d = 1; d <= maxDegree; d++) {
					functions.add(getStageEstimationFunction(trainSizes,
							trainDurations, d));
				}

				// test how well each function generalizes using the cross
				// validation set
				// the cross validation error is defined as: (1/2m) *
				// sum{1..m}(h(x_i)-y_i)^2
				// m = size of the cross validation set
				// h = the polynomial function we are evaluating (hypothesis)
				// x_i = the size of the data for the i-th stage in the cv set
				// y_i = the duration of the i-th stage in the cv set
				int m = testSizes.size();
				for (int d = 0; d < functions.size(); d++) {
					double testError = 0;
					for (int j = 0; j < m; j++) {
						testError = Math
								.pow((functions.get(d).value(testSizes.get(j)) - testDurations
										.get(j).doubleValue()), 2);
					}
					testError *= 1.0 / (2.0 * m);
					testErrors[d] += testError / (double) (k * repetitions);
				}

			}

			List<Double> testErrorList = Arrays.asList(ArrayUtils
					.toObject(testErrors));
			double minError = Collections.min(testErrorList);
			int bestDegree = testErrorList.indexOf(minError);
			selections[bestDegree]++;
			log += (bestDegree+1);
		}

		cvLogger.trace(log);
		// Select the model with the lowest k-fold cross validation error
		// (better
		// generalizes)
		List<Double> testErrorList = Arrays.asList(ArrayUtils
				.toObject(testErrors));
		double minError = Collections.min(testErrorList);
		int bestDegree = testErrorList.indexOf(minError);

		String selectionString = "";
		for (int n = 0; n < maxDegree; n++)
			selectionString += selections[n] + " ";
		logger.info("Stage: " + stageID + " Selections:" + selectionString
				+ " Selected: " + (bestDegree + 1));

		// Now that the degree for the stage is fixed we can re-train that
		// model using also the cv set
		List<Double> trainSizes = new ArrayList<Double>();
		List<Long> trainDurations = new ArrayList<Long>();
		for (String appId : trainSet) {
			trainSizes.add(sizes.get(appId));
			trainDurations.add(stageDurations.get(stageID).get(appId));
		}
		PolynomialFunction bestModel = getStageEstimationFunction(trainSizes,
				trainDurations, (bestDegree + 1));

		return bestModel;
	}

	private Set<Integer> filterExecutedStages() {
		Set<Integer> stages = new HashSet<>();
		// any app is fine for this
		String appId = sizes.keySet().iterator().next();
		for (int stageID : stageDurations.keySet())
			if (stageDurations.get(stageID).get(appId) > 0)
				stages.add(stageID);
		return stages;
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

	private void showSet(List<String> set) {
		for (String id : set)
			logger.debug("App: " + id + " Size: " + sizes.get(id));
	}

	private List<String> extractTrainSet() {
		List<String> availableApps = new ArrayList<>(sizes.keySet());
		availableApps.removeAll(testSet);
		return availableApps;
	}

	private List<String> extractTestSet() {

		List<String> tests = new ArrayList<String>();
		SortedMap<Double, String> size2application = new TreeMap<>();
		for (String id : sizes.keySet())
			size2application.put(sizes.get(id), id);

		int testSetSize = new Long(
				Math.round((sizes.size() * (1 - config.trainFraction))))
				.intValue();

		testSetSize = testSetSize < 1 ? 1 : testSetSize;

		List<Double> sortedSizes = new ArrayList<Double>(
				size2application.keySet());

		for (double size : sortedSizes.subList(sizes.size() - testSetSize,
				sizes.size()))
			tests.add(size2application.get(size));
		return tests;
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
			// skip non executed stages
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
