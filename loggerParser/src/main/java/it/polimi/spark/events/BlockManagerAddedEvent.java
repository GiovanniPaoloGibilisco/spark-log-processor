package it.polimi.spark.events;

public class BlockManagerAddedEvent extends SparkListenerEvent {
	private Long maxMemory;
	private Long timestamp;
	private String executorID;
	private String host;
	private int port;

	public Long getMaxMemory() {
		return maxMemory;
	}

	public void setMaxMemory(Long maxMemory) {
		this.maxMemory = maxMemory;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getExecutorID() {
		return executorID;
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
