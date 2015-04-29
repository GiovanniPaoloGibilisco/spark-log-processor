package it.polimi.spark.events;

public class RDDInfo {
	private int id;
	private String name;

	// Storage Level
	private boolean storageDisk;
	private boolean storageMemory;
	private boolean storageTachyon;
	private boolean storageDeserialized;
	private int storageReplication;

	private int numberOfPartitions;
	private int numberOfCachedPartitions;
	private long memorysize;
	private long tachyonsize;
	private long diskSize;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isStorageDisk() {
		return storageDisk;
	}

	public void setStorageDisk(boolean storageDisk) {
		this.storageDisk = storageDisk;
	}

	public boolean isStorageMemory() {
		return storageMemory;
	}

	public void setStorageMemory(boolean storageMemory) {
		this.storageMemory = storageMemory;
	}

	public boolean isStorageTachyon() {
		return storageTachyon;
	}

	public void setStorageTachyon(boolean storageTachyon) {
		this.storageTachyon = storageTachyon;
	}

	public boolean isStorageDeserialized() {
		return storageDeserialized;
	}

	public void setStorageDeserialized(boolean storageDeserialized) {
		this.storageDeserialized = storageDeserialized;
	}

	public int getStorageReplication() {
		return storageReplication;
	}

	public void setStorageReplication(int storageReplication) {
		this.storageReplication = storageReplication;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public int getNumberOfCachedPartitions() {
		return numberOfCachedPartitions;
	}

	public void setNumberOfCachedPartitions(int numberOfCachedPartitions) {
		this.numberOfCachedPartitions = numberOfCachedPartitions;
	}

	public long getMemorysize() {
		return memorysize;
	}

	public void setMemorysize(long memorysize) {
		this.memorysize = memorysize;
	}

	public long getTachyonsize() {
		return tachyonsize;
	}

	public void setTachyonsize(long tachyonsize) {
		this.tachyonsize = tachyonsize;
	}

	public long getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(long diskSize) {
		this.diskSize = diskSize;
	}

}
