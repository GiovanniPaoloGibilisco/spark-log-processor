package it.polimi.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDD {

	private String appID;
	private String clusterName;
	private int id;
	private String name;
	private int stageIDl;
	private String scope;
	private boolean useDisk;
	private boolean useMemory;
	private boolean deserialized;
	private int numberOfPartitions;
	private int cachedPartitions;
	private double memorySize;
	private double diskSize;

	static final Logger logger = LoggerFactory.getLogger(Stage.class);

	/**
	 * @param appID
	 * @param clusterName
	 * @param id
	 */
	public RDD(String appID, String clusterName, int id) {
		super();
		this.appID = appID;
		this.clusterName = clusterName;
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getStageID() {
		return stageIDl;
	}

	public void setStageIDl(int stageIDl) {
		this.stageIDl = stageIDl;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public boolean isUseDisk() {
		return useDisk;
	}

	public void setUseDisk(boolean useDisk) {
		this.useDisk = useDisk;
	}

	public boolean isUseMemory() {
		return useMemory;
	}

	public void setUseMemory(boolean useMemory) {
		this.useMemory = useMemory;
	}

	public boolean isDeserialized() {
		return deserialized;
	}

	public void setDeserialized(boolean deserialized) {
		this.deserialized = deserialized;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public int getNumberOfCachedPartitions() {
		return cachedPartitions;
	}

	public void setNumberOfCachedPartitions(int cachedPartitions) {
		this.cachedPartitions = cachedPartitions;
	}

	public double getMemorySize() {
		return memorySize;
	}

	public void setMemorySize(double memorySize) {
		this.memorySize = memorySize;
	}

	public double getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(double diskSize) {
		this.diskSize = diskSize;
	}

	public String getAppID() {
		return appID;
	}

	public String getClusterName() {
		return clusterName;
	}

	public int getID() {
		return id;
	}

}
