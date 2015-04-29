package it.polimi.spark.events;

public class ExecutorAddedEvent {
	private long timestamp;
	private String executorID;
	private ExecutorInfo executorInfo;

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getExecutorID() {
		return executorID;
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public ExecutorInfo getExecutorInfo() {
		return executorInfo;
	}

	public void setExecutorInfo(ExecutorInfo executorInfo) {
		this.executorInfo = executorInfo;
	}
}
