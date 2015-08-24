package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
	static long estimateJobDuration(
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
		// TODO: concatenate properly
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
}
