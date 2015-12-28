package it.polimi.spark.estimator.datagen;

import it.polimi.spark.dag.Stagenode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DagGenerator {

	String inputDag;
	static final Logger logger = LoggerFactory.getLogger(DagGenerator.class);

	public DagGenerator(String inputDag) {
		this.inputDag = inputDag;
	}

	public DirectedAcyclicGraph<Stagenode, DefaultEdge> buildDag()
			throws IOException {
		InputStream is = new FileInputStream(Paths.get(inputDag).toFile());
		String jsonTxt = IOUtils.toString(is);
		// Parse the input dag to build the list of stages
		JSONObject obj = new JSONObject(jsonTxt);
		JSONArray arr = obj.getJSONArray("stages");

		List<Stagenode> stages = new ArrayList<>();
		for (int i = 0; i < arr.length(); i++) {
			int jobId = arr.getJSONObject(i).getInt("JobId");
			int stageId = arr.getJSONObject(i).getInt("StageId");
			List<Integer> parentIds = new ArrayList<Integer>();
			if (arr.getJSONObject(i).has("parentStageIds")) {
				JSONArray parents = arr.getJSONObject(i).getJSONArray(
						"parentStageIds");

				for (int j = 0; j < parents.length(); j++) {
					parentIds.add(parents.getInt(j));
				}
			}

			stages.add(new Stagenode(jobId, stageId, parentIds, "", true));

		}
		is.close();

		//TODO: build one dag per job
		return buildStageDag(stages);

	}

	/**
	 * Builds a DAG using all the stages in the provided list.
	 * 
	 * @param stages
	 * @return
	 */
	private DirectedAcyclicGraph<Stagenode, DefaultEdge> buildStageDag(
			List<Stagenode> stages) {
		DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = new DirectedAcyclicGraph<Stagenode, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for stages quickly
		// and add vertexes to the graph
		HashMap<Integer, Stagenode> stageMap = new HashMap<Integer, Stagenode>(
				stages.size());
		for (Stagenode stage : stages) {
			stageMap.put(stage.getId(), stage);
			logger.debug("Adding Stage " + stage.getId() + " to the graph");
			dag.addVertex(stage);

		}

		// add all edges then
		for (Stagenode stage : stages) {
			if (stage.getParentIDs() != null)
				for (Integer source : stage.getParentIDs()) {
					logger.debug("Adding link from Stage " + source
							+ "to Stage" + stage.getId());
					dag.addEdge(stageMap.get(source), stage);
				}
		}
		return dag;
	}

}
