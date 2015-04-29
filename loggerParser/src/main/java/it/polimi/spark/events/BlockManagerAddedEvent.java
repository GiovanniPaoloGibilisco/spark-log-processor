package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class BlockManagerAddedEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerBlockManagerAdded";
	private String executorID;
	private String host;
	private Long maxMemory;
	private int port;

	private Long timestamp;

	public String getExecutorID() {
		return executorID;
	}

	public String getHost() {
		return host;
	}

	public Long getMaxMemory() {
		return maxMemory;
	}

	public int getPort() {
		return port;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public BlockManagerAddedEvent initialize(JsonObject jsonObject) {
		maxMemory = jsonObject.get("Maximum Memory").getAsLong();
		timestamp = jsonObject.get("Timestamp").getAsLong();
		JsonObject blockManagerObject = jsonObject.get("Block Manager ID").getAsJsonObject();
		executorID = blockManagerObject.get("Executor ID").getAsString();
		host = blockManagerObject.get("Host").getAsString();
		port = blockManagerObject.get("Port").getAsInt();
		return this;
		
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setMaxMemory(Long maxMemory) {
		this.maxMemory = maxMemory;
	}

	public void setPort(int port) {
		this.port = port;
	}

	
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	

}
