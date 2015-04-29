package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class TaskMetrics implements JsonInitializable {
	private long diskBytesSpilled;
	private long executorDeserializerTime;
	private long executorRunTime;
	private String hostName;
	private InputMetrics inputMetrics;
	private long jvmGCTime;
	private long memoryBytesSpilled;
	private long resultSerializationTime;
	private long resultSize;
	private ShuffleReadMetrics shuffleReadMetrics;
	private ShuffleWriteMetrics shuffleWriteMetrics;

	public long getDiskBytesSpilled() {
		return diskBytesSpilled;
	}

	public long getExecutorDeserializerTime() {
		return executorDeserializerTime;
	}

	public long getExecutorRunTime() {
		return executorRunTime;
	}

	public String getHostName() {
		return hostName;
	}

	public InputMetrics getInputMetrics() {
		return inputMetrics;
	}

	public long getJvmGCTime() {
		return jvmGCTime;
	}

	public long getMemoryBytesSpilled() {
		return memoryBytesSpilled;
	}

	public long getResultSerializationTime() {
		return resultSerializationTime;
	}

	public long getResultSize() {
		return resultSize;
	}

	public ShuffleReadMetrics getShuffleReadMetrics() {
		return shuffleReadMetrics;
	}

	public ShuffleWriteMetrics getShuffleWriteMetrics() {
		return shuffleWriteMetrics;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TaskMetrics initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		hostName = jsonObject.get("Host Name").getAsString();
		executorDeserializerTime = jsonObject.get("Executor Deserialize Time")
				.getAsLong();
		executorRunTime = jsonObject.get("Executor Run Time").getAsLong();
		resultSize = jsonObject.get("Result Size").getAsLong();
		jvmGCTime = jsonObject.get("JVM GC Time").getAsLong();
		resultSerializationTime = jsonObject.get("Result Serialization Time")
				.getAsLong();
		memoryBytesSpilled = jsonObject.get("Memory Bytes Spilled").getAsLong();
		diskBytesSpilled = jsonObject.get("Disk Bytes Spilled").getAsLong();
		if (jsonObject.get("Shuffle Read Metrics") != null)
			shuffleReadMetrics = new ShuffleReadMetrics().initialize(jsonObject
					.get("Shuffle Read Metrics").getAsJsonObject());
		if (jsonObject.get("Shuffle Write Metrics") != null)
			shuffleWriteMetrics = new ShuffleWriteMetrics()
					.initialize(jsonObject.get("Shuffle Write Metrics")
							.getAsJsonObject());
		if (jsonObject.get("Input Metrics") != null)
			inputMetrics = new InputMetrics().initialize((jsonObject
					.get("Input Metrics").getAsJsonObject()));
		return this;
	}

	public void setDiskBytesSpilled(long diskBytesSpilled) {
		this.diskBytesSpilled = diskBytesSpilled;
	}

	public void setExecutorDeserializerTime(long executorDeserializerTime) {
		this.executorDeserializerTime = executorDeserializerTime;
	}

	public void setExecutorRunTime(long executorRunTime) {
		this.executorRunTime = executorRunTime;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public void setInputMetrics(InputMetrics inputMetrics) {
		this.inputMetrics = inputMetrics;
	}

	public void setJvmGCTime(long jvmGCTime) {
		this.jvmGCTime = jvmGCTime;
	}

	public void setMemoryBytesSpilled(long memoryBytesSpilled) {
		this.memoryBytesSpilled = memoryBytesSpilled;
	}

	public void setResultSerializationTime(long resultSerializationTime) {
		this.resultSerializationTime = resultSerializationTime;
	}

	public void setResultSize(long resultSize) {
		this.resultSize = resultSize;
	}

	public void setShuffleReadMetrics(ShuffleReadMetrics shuffleReadMetrics) {
		this.shuffleReadMetrics = shuffleReadMetrics;
	}

	public void setShuffleWriteMetrics(ShuffleWriteMetrics shuffleWriteMetrics) {
		this.shuffleWriteMetrics = shuffleWriteMetrics;
	}
}
