package it.polimi.spark.dag;

import java.io.Serializable;
import java.util.List;

public class RDDnode implements Serializable {

	private int id;
	private String name;
	private List<Integer> parentIDs;
	private int partitions;
	private int scopeID;
	private String scopeName;
	private int stageID;
	private boolean useDisk;
	private boolean useMemory;
	private boolean useExternalBlockStore;
	private boolean deserialized;
	private int replication;	
	


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
	public RDDnode(int id, String name, List<Integer> parentIDs, int partitions,
			int scopeID, String scopeName, int stageID, boolean useDisk,
			boolean useMemory, boolean useExternalBlockStore,
			boolean deserialized, int replication) {
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

	public int getReplication() {
		return replication;
	}

	public int getStageID() {
		return stageID;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public List<Integer> getParentIDs() {
		return parentIDs;
	}

	public int getPartitions() {
		return partitions;
	}

	public int getScopeID() {
		return scopeID;
	}

	public String getScopeName() {
		return scopeName;
	}

}
