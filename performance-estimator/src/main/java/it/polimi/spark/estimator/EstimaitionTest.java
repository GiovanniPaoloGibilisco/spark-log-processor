package it.polimi.spark.estimator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

public class EstimaitionTest {
	static final double min = 0.5;
	static final double max = 0.8;
	static final String inputFolder = "src/main/resources/LogisticRegressionDisk";
	static final String baseOut = "src/main/resources/output";

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, SQLException {

		SortedMap<Double, SortedMap<Double, Long>> errorMaps = new TreeMap<Double, SortedMap<Double, Long>>();
		SortedMap<Double, SortedMap<Double, Double>> relativeErrorMaps = new TreeMap<Double, SortedMap<Double, Double>>();
		SortedMap<Double, SortedMap<Double, Long>> durationMaps = new TreeMap<Double, SortedMap<Double, Long>>();
		SortedMap<Double, SortedMap<Double, Long>> estimatedDurationMaps = new TreeMap<Double, SortedMap<Double, Long>>();
		SortedMap<Double, String> estimationFunctions = new TreeMap<Double, String>();
		FileUtils.forceMkdir(Paths.get(baseOut).toFile());
		for (double trainingPercentage = min; trainingPercentage <= max; trainingPercentage += 0.1) {

			DecimalFormat df = new DecimalFormat("#.##");
			trainingPercentage = Double.valueOf(df.format(trainingPercentage));
			System.out.println("Training: " + trainingPercentage);
			String outputFolder = Paths.get(baseOut, "" + trainingPercentage)
					.toString();
			// double trainingPercentage = 0.6;
			String[] arguments = new String[6];
			arguments[0] = "-b";
			arguments[1] = inputFolder;
			arguments[2] = "-t";
			arguments[3] = "" + trainingPercentage;
			arguments[4] = "-o";
			arguments[5] = outputFolder;
			Estimator.main(arguments);
			List<EstimationResult> results = Estimator.results;

			if (!errorMaps.containsKey(trainingPercentage))
				errorMaps.put(trainingPercentage, new TreeMap<Double, Long>());
			if (!relativeErrorMaps.containsKey(trainingPercentage))
				relativeErrorMaps.put(trainingPercentage,
						new TreeMap<Double, Double>());
			if (!durationMaps.containsKey(trainingPercentage))
				durationMaps.put(trainingPercentage,
						new TreeMap<Double, Long>());
			if (!estimatedDurationMaps.containsKey(trainingPercentage))
				estimatedDurationMaps.put(trainingPercentage,
						new TreeMap<Double, Long>());
			if (!estimationFunctions.containsKey(trainingPercentage))
				estimationFunctions.put(trainingPercentage,
						Estimator.estimationFunction);

			for (EstimationResult res : results) {
				errorMaps.get(trainingPercentage).put(res.getAppSize(),
						res.getError());
				relativeErrorMaps.get(trainingPercentage).put(res.getAppSize(),
						res.getRelativeError());
				durationMaps.get(trainingPercentage).put(res.getAppSize(),
						res.getRealDuration());
				estimatedDurationMaps.get(trainingPercentage).put(
						res.getAppSize(), res.getEstimatedDuration());
			}

		}

		saveMaptoFile("Errors.csv", errorMaps);
		saveMaptoFile("RelativeErrors.csv", relativeErrorMaps);
		saveMaptoFile("Durations.csv", durationMaps);
		saveMaptoFile("EstimatedDurations.csv", estimatedDurationMaps);
		saveToFileGrouped("GroupedResults.csv", durationMaps,
				estimatedDurationMaps, errorMaps, relativeErrorMaps);

		for (double trainingPercentage = min; trainingPercentage <= max; trainingPercentage += 0.1) {
			DecimalFormat df = new DecimalFormat("#.##");
			trainingPercentage = Double.valueOf(df.format(trainingPercentage));
			Path scriptPath = Paths.get(baseOut, "" + trainingPercentage,
					"EstimationScript.m");
			exportMatlabScript(
					new ArrayList<Double>(durationMaps.get(trainingPercentage)
							.keySet()),
					new ArrayList<Long>(durationMaps.get(trainingPercentage)
							.values()),
					estimationFunctions.get(trainingPercentage),
					trainingPercentage, scriptPath);
		}
	}

	private static <T> void saveMaptoFile(String filename,
			SortedMap<Double, SortedMap<Double, T>> inputMap)
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException {
		// save relative errors
		OutputStream os = new FileOutputStream(Paths.get(baseOut, filename)
				.toString());
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		// the schema first
		br.write("Training Perc,");
		for (double size : inputMap.get(min).keySet())
			br.write(size + "GB,");
		br.write("\n");

		for (double trainingPerc : inputMap.keySet()) {
			br.write(trainingPerc + ",");
			for (double size : inputMap.get(min).keySet()) {
				if (!inputMap.get(trainingPerc).containsKey(size))
					br.write(",");
				else
					br.write(inputMap.get(trainingPerc).get(size) + ",");
			}
			br.write("\n");
		}
		br.flush();
		br.close();
	}

	private static <T, K> void saveToFileGrouped(String filename,
			SortedMap<Double, SortedMap<Double, T>> durationMap,
			SortedMap<Double, SortedMap<Double, T>> estimatedDurationMap,
			SortedMap<Double, SortedMap<Double, T>> errorMap,
			SortedMap<Double, SortedMap<Double, K>> relativeErrorMap)
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException {
		// save relative errors
		OutputStream os = new FileOutputStream(Paths.get(baseOut, filename)
				.toString());
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		// the schema first
		br.write("Training Perc,Application Size,Duration, EstimatedDuration,Error, RelativeError");
		br.write("\n");

		for (double trainingPerc : durationMap.keySet()) {

			for (double size : durationMap.get(min).keySet()) {
				br.write(trainingPerc + ",");
				// size
				br.write(size + ",");
				// duration
				if (!durationMap.get(trainingPerc).containsKey(size))
					br.write(",");
				else
					br.write(durationMap.get(trainingPerc).get(size) + ",");
				// estimated duration
				if (!estimatedDurationMap.get(trainingPerc).containsKey(size))
					br.write(",");
				else
					br.write(estimatedDurationMap.get(trainingPerc).get(size)
							+ ",");
				// error
				if (!errorMap.get(trainingPerc).containsKey(size))
					br.write(",");
				else
					br.write(errorMap.get(trainingPerc).get(size) + ",");
				// relativerror
				if (!relativeErrorMap.get(trainingPerc).containsKey(size))
					br.write(",");
				else
					br.write(relativeErrorMap.get(trainingPerc).get(size) + ",");
				br.write("\n");
			}

		}
		br.flush();
		br.close();

	}

	private static void exportMatlabScript(List<Double> sizes,
			List<Long> durations, String estimationFunction,
			double trainingPercentage, Path filePath) throws IOException {
		OutputStream os = new FileOutputStream(filePath.toString());
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));

		// export the sizes as 'x'
		String x_var = "x = [";
		for (double size : sizes)
			x_var += size + " ";
		x_var += "];";
		br.write(x_var);
		br.write("\n");

		// export the durations as 'y'
		String y_var = "y = [";
		for (double duration : durations)
			y_var += duration + " ";
		y_var += "];";
		br.write(y_var);
		br.write("\n");

		// export the estimation function and apply it t generate y_estimate
		// matlab likes to know explicitly the multiplication operation
		estimationFunction = estimationFunction.replaceAll(" x", "*x");
		// also we should say that ^ has to be applied element wise
		estimationFunction = estimationFunction.replaceAll("\\^", ".^");
		String y_estimate_var = "y_estimate = " + estimationFunction +";";
		br.write(y_estimate_var);
		br.write("\n");

		// export the trainign percentage
		br.write("training_percentage = " + trainingPercentage+";");
		br.write("\n");
		br.write("addpath(\'../../\')"+"\n");
		br.write("EstimationAnalysis(x,y,y_estimate,training_percentage)"+"\n");

		br.flush();
		br.close();
		os.close();
	}
}
