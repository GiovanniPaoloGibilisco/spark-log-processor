package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class RDDInfo implements JsonInitializable{
	private long diskSize;
	private int id;

	private long memorysize;
	private String name;
	private int numberOfCachedPartitions;
	private int numberOfPartitions;
	private boolean storageDeserialized;

	// Storage Level
	private boolean storageDisk;
	private boolean storageMemory;
	private int storageReplication;
	private boolean storageTachyon;
	private long tachyonsize;

	public long getDiskSize() {
		return diskSize;
	}

	public int getId() {
		return id;
	}

	public long getMemorysize() {
		return memorysize;
	}

	public String getName() {
		return name;
	}

	public int getNumberOfCachedPartitions() {
		return numberOfCachedPartitions;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public int getStorageReplication() {
		return storageReplication;
	}

	public long getTachyonsize() {
		return tachyonsize;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RDDInfo initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		id = jsonObject.get("RDD ID").getAsInt();
		name = jsonObject.get("Name").getAsString();
		JsonObject storageObject = jsonObject.get("Storage Level").getAsJsonObject();
		storageDisk = storageObject.get("Use Disk").getAsBoolean();
		storageMemory= storageObject.get("Use Memory").getAsBoolean();
		storageTachyon = storageObject.get("Use Tachyon").getAsBoolean();
		storageDeserialized = storageObject.get("Deserialized").getAsBoolean();
		storageReplication= storageObject.get("Replication").getAsInt();
		numberOfPartitions = jsonObject.get("Number of Partitions").getAsInt();
		numberOfCachedPartitions= jsonObject.get("Number of Cached Partitions").getAsInt();
		memorysize = jsonObject.get("Memory Size").getAsLong();
		tachyonsize = jsonObject.get("Tachyon Size").getAsLong();
		diskSize = jsonObject.get("Disk Size").getAsLong();
		return this;
	}

	public boolean isStorageDeserialized() {
		return storageDeserialized;
	}

	public boolean isStorageDisk() {
		return storageDisk;
	}

	public boolean isStorageMemory() {
		return storageMemory;
	}

	public boolean isStorageTachyon() {
		return storageTachyon;
	}

	public void setDiskSize(long diskSize) {
		this.diskSize = diskSize;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setMemorysize(long memorysize) {
		this.memorysize = memorysize;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setNumberOfCachedPartitions(int numberOfCachedPartitions) {
		this.numberOfCachedPartitions = numberOfCachedPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public void setStorageDeserialized(boolean storageDeserialized) {
		this.storageDeserialized = storageDeserialized;
	}

	public void setStorageDisk(boolean storageDisk) {
		this.storageDisk = storageDisk;
	}

	public void setStorageMemory(boolean storageMemory) {
		this.storageMemory = storageMemory;
	}

	public void setStorageReplication(int storageReplication) {
		this.storageReplication = storageReplication;
	}

	public void setStorageTachyon(boolean storageTachyon) {
		this.storageTachyon = storageTachyon;
	}

	public void setTachyonsize(long tachyonsize) {
		this.tachyonsize = tachyonsize;
	}

}
