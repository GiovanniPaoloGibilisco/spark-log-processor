package it.polimi.spark.events;
import com.google.gson.JsonObject;

public class ShuffleWriteMetrics implements JsonInitializable {

	private long bytesWritten;
	private int recordsWritten;
	private long writeTime;

	public long getBytesWritten() {
		return bytesWritten;
	}

	public int getRecordsWritten() {
		return recordsWritten;
	}

	public long getWriteTime() {
		return writeTime;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ShuffleWriteMetrics initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		bytesWritten = jsonObject.get("Shuffle Bytes Written").getAsLong();
		writeTime = jsonObject.get("Shuffle Write Time").getAsLong();
		recordsWritten = jsonObject.get("Shuffle Records Written").getAsInt();
		return this;
	}

	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}

	public void setRecordsWritten(int recordsWritten) {
		this.recordsWritten = recordsWritten;
	}

	public void setWriteTime(long writeTime) {
		this.writeTime = writeTime;
	}

}
