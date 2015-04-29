package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class ShuffleReadMetrics implements JsonInitializable {
	private long fetchWaitTime;
	private int localBlocksFetched;
	private long localBytesRead;
	private int remoteBlocksFetched;
	private long remoteBytesRead;
	private long totalRecordsRead;

	public long getFetchWaitTime() {
		return fetchWaitTime;
	}

	public int getLocalBlocksFatched() {
		return localBlocksFetched;
	}

	public long getLocalBytesRead() {
		return localBytesRead;
	}

	public int getRemoteBlocksFetched() {
		return remoteBlocksFetched;
	}

	public long getRemoteBytesRead() {
		return remoteBytesRead;
	}

	public long getTotalRecordsRead() {
		return totalRecordsRead;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ShuffleReadMetrics initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		remoteBlocksFetched = jsonObject.get("Remote Blocks Fetched")
				.getAsInt();
		localBlocksFetched = jsonObject.get("Local Blocks Fetched").getAsInt();
		fetchWaitTime = jsonObject.get("Fetch Wait Time").getAsLong();
		remoteBytesRead = jsonObject.get("Remote Bytes Read").getAsLong();
		localBytesRead = jsonObject.get("Local Bytes Read").getAsLong();
		totalRecordsRead = jsonObject.get("Total Records Read").getAsLong();
		return this;
	}

	public void setFetchWaitTime(long fetchWaitTime) {
		this.fetchWaitTime = fetchWaitTime;
	}

	public void setLocalBlocksFatched(int localBlocksFatched) {
		this.localBlocksFetched = localBlocksFatched;
	}

	public void setLocalBytesRead(long localBytesRead) {
		this.localBytesRead = localBytesRead;
	}

	public void setRemoteBlocksFetched(int remoteBlocksFetched) {
		this.remoteBlocksFetched = remoteBlocksFetched;
	}

	public void setRemoteBytesRead(long remoteBytesRead) {
		this.remoteBytesRead = remoteBytesRead;
	}

	public void setTotalRecordsRead(long totalRecordsRead) {
		this.totalRecordsRead = totalRecordsRead;
	}

}
