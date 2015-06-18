package it.polimi.spark.dag;

import java.io.Serializable;
import java.util.List;

public class Stagenode implements Serializable {
	int jobId;
	int id;
	private List<Integer> parentIDs;
	String name;
	private boolean executed;

	public Stagenode(int jobId, int id, List<Integer> parentIDs, String name,
			boolean executed) {
		super();
		this.jobId = jobId;
		this.id = id;
		this.parentIDs = parentIDs;
		this.name = name;
		this.executed = executed;
	}

	public int getJobId() {
		return jobId;
	}

	public int getId() {
		return id;
	}

	public List<Integer> getParentIDs() {
		return parentIDs;
	}

	public String getName() {
		return name;
	}

	public boolean isExecuted() {
		return executed;
	}

}
