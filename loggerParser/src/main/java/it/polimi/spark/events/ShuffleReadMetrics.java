package it.polimi.spark.events;

public class ShuffleReadMetrics {
	private int remoteBlocksFetched;
	private int localBlocksFatched;
	private long fetchWaitTime;
	private long remoteBytesRead;
	private long localBytesRead;
	private long totalRecordsRead;

	public int getRemoteBlocksFetched() {
		return remoteBlocksFetched;
	}

	public void setRemoteBlocksFetched(int remoteBlocksFetched) {
		this.remoteBlocksFetched = remoteBlocksFetched;
	}

	public int getLocalBlocksFatched() {
		return localBlocksFatched;
	}

	public void setLocalBlocksFatched(int localBlocksFatched) {
		this.localBlocksFatched = localBlocksFatched;
	}

	public long getFetchWaitTime() {
		return fetchWaitTime;
	}

	public void setFetchWaitTime(long fetchWaitTime) {
		this.fetchWaitTime = fetchWaitTime;
	}

	public long getRemoteBytesRead() {
		return remoteBytesRead;
	}

	public void setRemoteBytesRead(long remoteBytesRead) {
		this.remoteBytesRead = remoteBytesRead;
	}

	public long getLocalBytesRead() {
		return localBytesRead;
	}

	public void setLocalBytesRead(long localBytesRead) {
		this.localBytesRead = localBytesRead;
	}

	public long getTotalRecordsRead() {
		return totalRecordsRead;
	}

	public void setTotalRecordsRead(long totalRecordsRead) {
		this.totalRecordsRead = totalRecordsRead;
	}

}
