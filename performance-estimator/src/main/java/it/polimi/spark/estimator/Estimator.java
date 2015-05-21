package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stage;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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
		Path inputFolder = Paths.get(config.inputFolder);
		if (!inputFolder.toFile().exists()) {
			logger.info("Input folder " + inputFolder + " does not exist");
			return;
		}

		Map<String, DirectedAcyclicGraph<Stage, DefaultEdge>> stageDags = new HashMap<String, DirectedAcyclicGraph<Stage, DefaultEdge>>();

		if (inputFolder.toFile().isFile()) {
			logger.info("Input folder is actually a file, processing only that file");
			DirectedAcyclicGraph<Stage, DefaultEdge> dag = deserializeFile(inputFolder);
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
					DirectedAcyclicGraph<Stage, DefaultEdge> dag = deserializeFile(file);
					if (dag != null)
						stageDags.put(file.getFileName().toString(), dag);
				}
			}
		}
		logger.info("Loaded " + stageDags.size() + " dags");

		for (String dagName : stageDags.keySet()) {
			DirectedAcyclicGraph<Stage, DefaultEdge> dag = stageDags
					.get(dagName);
			logger.info("Dag " + dagName + " has: " + dag.vertexSet().size()
					+ " vertexes and " + dag.edgeSet().size() + " edges");
		}

	}

	private static DirectedAcyclicGraph<Stage, DefaultEdge> deserializeFile(
			Path file) throws IOException, ClassNotFoundException {
		InputStream fileIn = Files.newInputStream(file);
		DirectedAcyclicGraph<Stage, DefaultEdge> dag = null;
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(fileIn);
			dag = (DirectedAcyclicGraph<Stage, DefaultEdge>) in.readObject();
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
}
