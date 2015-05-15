package it.polimi.spark;

import java.util.List;

public class RDD {

	private Long id;
	private String name;
	private List<Long> parentIDs;
	private Long partitions;
	private Long scopeID;
	private String scopeName;
	private Long stageID;

	/**
	 * @param id
	 * @param name
	 * @param parentIDs
	 * @param partitions
	 * @param scopeID
	 * @param scopeName
	 * @param stageID
	 */
	public RDD(Long id, String name, List<Long> parentIDs, Long partitions,
			Long scopeID, String scopeName, Long stageID) {
		super();
		this.id = id;
		this.name = name;
		this.parentIDs = parentIDs;
		this.partitions = partitions;
		this.scopeID = scopeID;
		this.scopeName = scopeName;
		this.stageID = stageID;
	}

	public Long getStageID() {
		return stageID;
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public List<Long> getParentIDs() {
		return parentIDs;
	}

	public Long getPartitions() {
		return partitions;
	}

	public Long getScopeID() {
		return scopeID;
	}

	public String getScopeName() {
		return scopeName;
	}

}
