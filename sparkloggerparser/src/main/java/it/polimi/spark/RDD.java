package it.polimi.spark;

import java.util.List;

public class RDD {

	private long id;
	private String name;
	private List<Long> parentIDs;
	private long partitions;
	private long scopeID;
	private String scopeName;
	private long stageID;
	private boolean useDisk;
	private boolean useMemory;
	private boolean useExternalBlockStore;
	private boolean deserialized;
	private long replication;	
	


	/**
	 * @param id
	 * @param name
	 * @param parentIDs
	 * @param partitions
	 * @param scopeID
	 * @param scopeName
	 * @param stageID
	 * @param useDisk
	 * @param useMemory
	 * @param useExternalBlockStore
	 * @param deserialized
	 * @param replication
	 */
	public RDD(long id, String name, List<Long> parentIDs, long partitions,
			long scopeID, String scopeName, long stageID, boolean useDisk,
			boolean useMemory, boolean useExternalBlockStore,
			boolean deserialized, long replication) {
		super();
		this.id = id;
		this.name = name;
		this.parentIDs = parentIDs;
		this.partitions = partitions;
		this.scopeID = scopeID;
		this.scopeName = scopeName;
		this.stageID = stageID;
		this.useDisk = useDisk;
		this.useMemory = useMemory;
		this.useExternalBlockStore = useExternalBlockStore;
		this.deserialized = deserialized;
		this.replication = replication;
	}

	public boolean isUseDisk() {
		return useDisk;
	}

	public boolean isUseMemory() {
		return useMemory;
	}

	public boolean isUseExternalBlockStore() {
		return useExternalBlockStore;
	}

	public boolean isDeserialized() {
		return deserialized;
	}

	public long getReplication() {
		return replication;
	}

	public long getStageID() {
		return stageID;
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public List<Long> getParentIDs() {
		return parentIDs;
	}

	public long getPartitions() {
		return partitions;
	}

	public long getScopeID() {
		return scopeID;
	}

	public String getScopeName() {
		return scopeName;
	}

}
