package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class TaskInfo implements JsonInitializable {
	private int attempt;
	private String executorID;
	private boolean failed;
	private long finishTime;
	private long gettingResultTime;
	private String host;
	private int index;
	private long launchTime;
	private String locality;
	private boolean speculative;
	private int taskID;

	public int getAttempt() {
		return attempt;
	}

	public String getExecutorID() {
		return executorID;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public long getGettingResultTime() {
		return gettingResultTime;
	}

	public String getHost() {
		return host;
	}

	public int getIndex() {
		return index;
	}

	public long getLaunchTime() {
		return launchTime;
	}

	public String getLocality() {
		return locality;
	}

	// TODO: find out what the accumulables tag in the log means
	public int getTaskID() {
		return taskID;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TaskInfo initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		taskID = jsonObject.get("Task ID").getAsInt();
		index = jsonObject.get("Index").getAsInt();
		attempt = jsonObject.get("Attempt").getAsInt();
		launchTime = jsonObject.get("Launch Time").getAsLong();
		executorID = jsonObject.get("Executor ID").getAsString();
		host = jsonObject.get("Host").getAsString();
		locality = jsonObject.get("Locality").getAsString();
		speculative = jsonObject.get("Speculative").getAsBoolean();
		gettingResultTime = jsonObject.get("Getting Result Time").getAsLong();
		finishTime = jsonObject.get("Finish Time").getAsLong();
		failed = jsonObject.get("Failed").getAsBoolean();
		return this;
	}

	public boolean isFailed() {
		return failed;
	}

	public boolean isSpeculative() {
		return speculative;
	}

	public void setAttempt(int attempt) {
		this.attempt = attempt;
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public void setFailed(boolean failed) {
		this.failed = failed;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public void setGettingResultTime(long gettingResultTime) {
		this.gettingResultTime = gettingResultTime;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public void setLaunchTime(long launchTime) {
		this.launchTime = launchTime;
	}

	public void setLocality(String locality) {
		this.locality = locality;
	}

	public void setSpeculative(boolean speculative) {
		this.speculative = speculative;
	}

	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}

}
