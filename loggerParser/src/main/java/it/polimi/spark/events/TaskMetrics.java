package it.polimi.spark.events;

public class TaskMetrics {
	private String hostName;
	private long executorDeserializerTime;
	private long executorRunTime;
	private long resultSize;
	private long jvmGCTime;
	private long resultSerializationTime;
	private long memoryBytesSpilled;
	private ShuffleReadMetrics shuffleReadMetrics;

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public long getExecutorDeserializerTime() {
		return executorDeserializerTime;
	}

	public void setExecutorDeserializerTime(long executorDeserializerTime) {
		this.executorDeserializerTime = executorDeserializerTime;
	}

	public long getExecutorRunTime() {
		return executorRunTime;
	}

	public void setExecutorRunTime(long executorRunTime) {
		this.executorRunTime = executorRunTime;
	}

	public long getResultSize() {
		return resultSize;
	}

	public void setResultSize(long resultSize) {
		this.resultSize = resultSize;
	}

	public long getJvmGCTime() {
		return jvmGCTime;
	}

	public void setJvmGCTime(long jvmGCTime) {
		this.jvmGCTime = jvmGCTime;
	}

	public long getResultSerializationTime() {
		return resultSerializationTime;
	}

	public void setResultSerializationTime(long resultSerializationTime) {
		this.resultSerializationTime = resultSerializationTime;
	}

	public long getMemoryBytesSpilled() {
		return memoryBytesSpilled;
	}

	public void setMemoryBytesSpilled(long memoryBytesSpilled) {
		this.memoryBytesSpilled = memoryBytesSpilled;
	}

	public ShuffleReadMetrics getShuffleReadMetrics() {
		return shuffleReadMetrics;
	}

	public void setShuffleReadMetrics(ShuffleReadMetrics shuffleReadMetrics) {
		this.shuffleReadMetrics = shuffleReadMetrics;
	}
}
