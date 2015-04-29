package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class ExecutorAddedEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerExecutorAdded";
	private String executorID;
	private ExecutorInfo executorInfo;

	private long timestamp;

	public String getExecutorID() {
		return executorID;
	}

	public ExecutorInfo getExecutorInfo() {
		return executorInfo;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ExecutorAddedEvent initialize(JsonObject jsonObject) {
		timestamp = jsonObject.get("Timestamp").getAsLong();
		executorID = jsonObject.get("Executor ID").getAsString();
		executorInfo = new ExecutorInfo();
		executorInfo.initialize(jsonObject.get("Executor Info")
				.getAsJsonObject());
		return this;
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public void setExecutorInfo(ExecutorInfo executorInfo) {
		this.executorInfo = executorInfo;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
}
