package it.polimi.spark.estimator.datagen;

import it.polimi.spark.dag.Stagenode;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationGenerator {

	// Configurations

	static final String baseFolder = "src/main/resources/FakeApp";
	static final Logger logger = LoggerFactory
			.getLogger(ApplicationGenerator.class);
	static final int applicationsNumber = 20;
	static final double minData = 1;
	static final double maxData = 200;
	static final double noisePercentage = 0.1;
	static final String appName = "FakeApp";
	static Map<Double, Map<Integer, Long>> stageDurations;

	public static void main(String[] args) throws IOException {

		DagGenerator dagGenerator = new DagGenerator(Paths.get(baseFolder,
				"inputDag.json").toString());

		DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = dagGenerator
				.buildDag();

		Set<Stagenode> stages = dag.vertexSet();

		Map<Integer, PolynomialFunction> stageGrowthMap = loadStageDurationFunctions(Paths
				.get(baseFolder, "inputDag.json").toString());

		initStageGrowthDurations(stageGrowthMap, noisePercentage);

		for (int i = 0; i <= applicationsNumber; i++) {

			// build the app id and create the folder
			String appID = "app-" + i;
			Path baseAppFolder = Paths.get(baseFolder, appID);
			Files.createDirectory(baseAppFolder);
			double dataSize = minData + i * (maxData - minData)
					/ applicationsNumber;

			// duration in ms
			// TODO: optionally add white noise.
			// TODO: aggregate stage durations
			long duration = aggregateJobDurations(dataSize, dag);

			// build the application.info File
			buildApplicationInfo(baseAppFolder, appID, appName, dataSize);

			// build application.csv file
			buildApplicationInfoCsv(baseAppFolder, appID, duration);

			// build StageDetails.csv file
			buildStageDetailsCsv(baseAppFolder, stages, dataSize);

			buildJobDetailsCsv(baseAppFolder, stages, duration);

			// save the dags
			saveDags(baseAppFolder, dag);

		}

	}

	private static void initStageGrowthDurations(
			Map<Integer, PolynomialFunction> stageGrowthMap, double randomFactor) {

		stageDurations = new HashMap<Double, Map<Integer, Long>>();

		for (int i = 0; i <= applicationsNumber; i++) {

			double dataSize = minData + i * (maxData - minData)
					/ applicationsNumber;

			if (!stageDurations.containsKey(dataSize))
				stageDurations.put(dataSize, new HashMap<Integer, Long>());
			for (int stageId : stageGrowthMap.keySet()) {
				long baseDuration = (long) stageGrowthMap.get(stageId).value(
						dataSize);

				Random random = new Random();
				double noisePercentage = random.nextGaussian() * randomFactor;
				long duration = (long) (baseDuration + baseDuration * noisePercentage);
				stageDurations.get(dataSize).put(stageId, (long) duration);
				logger.info("Clean Duration: "+baseDuration+" Noisy duration: "+duration);
			}

		}
	}

	private static void saveDags(Path baseAppFolder,
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag)
			throws IOException {

		Path dagFolders = Paths.get(baseAppFolder.toString(), "dags");
		Files.createDirectory(dagFolders);
		OutputStream os = new FileOutputStream(Paths.get(dagFolders.toString(),
				"Job_0").toString());
		ObjectOutputStream objectStream = new ObjectOutputStream(os);
		objectStream.writeObject(dag);
		objectStream.close();
		os.close();

	}

	private static long aggregateJobDurations(double dataSize,
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag) {

		// TODO: this is just a sum, we could use the dag and perfrom the same
		// aggregtion we use in the estimation.
		long duration = 0;
		for (Stagenode stage : dag.vertexSet())
			duration += getStageDuration(dataSize, stage.getId());

		return duration;
	}

	private static Map<Integer, PolynomialFunction> loadStageDurationFunctions(
			String inputDag) throws IOException {
		InputStream is = new FileInputStream(Paths.get(inputDag).toFile());
		String jsonTxt = IOUtils.toString(is);
		// Parse the input dag to build the list of stages
		JSONObject obj = new JSONObject(jsonTxt);
		JSONArray arr = obj.getJSONArray("stages");

		Map<Integer, PolynomialFunction> stageGrowthMap = new HashMap<Integer, PolynomialFunction>();
		for (int i = 0; i < arr.length(); i++) {
			int stageId = arr.getJSONObject(i).getInt("StageId");
			int polyExpFactor = arr.getJSONObject(i).getInt("polyExp");

			JSONArray factorsArr = arr.getJSONObject(i).getJSONArray("factors");
			double[] factors = new double[polyExpFactor + 1];
			for (int j = 0; j < factorsArr.length(); j++) {
				factors[polyExpFactor - j] = factorsArr.getDouble(j);
			}

			stageGrowthMap.put(stageId, new PolynomialFunction(factors));

		}
		is.close();

		return stageGrowthMap;
	}

	private static void buildStageDetailsCsv(Path baseAppFolder,
			Set<Stagenode> stages, double size) throws IOException {
		OutputStream infoOs = new FileOutputStream(Paths.get(
				baseAppFolder.toString(), "StageDetails.csv").toString());
		BufferedWriter infoBr = new BufferedWriter(new OutputStreamWriter(
				infoOs, "UTF-8"));

		final int fakeTaskNumber = 100;
		final int fakeInitialTime = 0;
		final String executed = "true";

		infoBr.write("Stage ID,Stage Name,Parent IDs,Number of Tasks,Submission Time,Completion Time,Duration,Executed,\n");

		for (Stagenode stage : stages) {
			String parentIds = "";
			for (int id : stage.getParentIDs())
				parentIds += id + " ";
			long stageDuration = getStageDuration(size, stage.getId());

			infoBr.write(stage.getId() + "," + stage.getName() + ","
					+ parentIds + "," + fakeTaskNumber + "," + fakeInitialTime
					+ "," + stageDuration + ","
					+ (fakeInitialTime + stageDuration) + "," + executed + ","
					+ "\n");
		}

		infoBr.flush();
		infoBr.close();
		infoOs.close();

	}

	private static void buildApplicationInfoCsv(Path baseAppFolder,
			String appID, long duration) throws IOException {
		OutputStream infoOs = new FileOutputStream(Paths.get(
				baseAppFolder.toString(), "application.csv").toString());
		BufferedWriter infoBr = new BufferedWriter(new OutputStreamWriter(
				infoOs, "UTF-8"));

		infoBr.write("Event,App ID,Timestamp,\n");
		infoBr.write("SparkListenerApplicationStart," + appID + ",0\n");
		infoBr.write("SparkListenerApplicationEnd," + appID + "," + duration
				+ "\n");
		infoBr.flush();
		infoBr.close();
		infoOs.close();
	}

	private static void buildJobDetailsCsv(Path baseAppFolder,
			Set<Stagenode> stages, long duration) throws IOException {
		OutputStream infoOs = new FileOutputStream(Paths.get(
				baseAppFolder.toString(), "JobDetails.csv").toString());
		BufferedWriter infoBr = new BufferedWriter(new OutputStreamWriter(
				infoOs, "UTF-8"));

		// TODO: implement the aggregation to get job durations here

		infoBr.write("Job ID,Submission Time,Completion Time,Duration,Stage IDs,maxStageID,minStageID,\n");
		infoBr.write("0,0," + duration + "," + duration + ",0 1 ,1,0,\n");

		infoBr.flush();
		infoBr.close();
		infoOs.close();

	}

	private static void buildApplicationInfo(Path baseAppFolder, String appID,
			String appName, double dataSize) throws IOException {
		OutputStream infoOs = new FileOutputStream(Paths.get(
				baseAppFolder.toString(), "application.info").toString());
		BufferedWriter infoBr = new BufferedWriter(new OutputStreamWriter(
				infoOs, "UTF-8"));

		infoBr.write("Cluster name;FakeApplicationCluster\n");
		infoBr.write("Application Id;" + appID + "\n");
		infoBr.write("Application Name;" + appName + "\n");
		infoBr.write("Database User;none\n");
		infoBr.write("Data Size;" + dataSize + "\n");

		infoBr.flush();
		infoBr.close();
		infoOs.close();
	}

	private static long getStageDuration(double dataSize, int stageId) {
		return stageDurations.get(dataSize).get(stageId);
	}

}
