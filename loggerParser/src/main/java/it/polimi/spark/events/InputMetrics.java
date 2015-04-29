package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class InputMetrics implements JsonInitializable {

	private long bytesRead;
	private String dataReadMethod;
	private int recordsRead;

	public long getBytesRead() {
		return bytesRead;
	}

	public String getDataReadMethod() {
		return dataReadMethod;
	}

	public int getRecordsRead() {
		return recordsRead;
	}

	@SuppressWarnings("unchecked")
	@Override
	public InputMetrics initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		dataReadMethod = jsonObject.get("Data Read Method").getAsString();
		bytesRead = jsonObject.get("Bytes Read").getAsLong();
		recordsRead = jsonObject.get("Records Read").getAsInt();
		return this;
	}

	public void setBytesRead(long bytesRead) {
		this.bytesRead = bytesRead;
	}

	public void setDataReadMethod(String dataReadMethod) {
		this.dataReadMethod = dataReadMethod;
	}

	public void setRecordsRead(int recordsRead) {
		this.recordsRead = recordsRead;
	}

}
