package it.polimi.spark;

import java.util.List;

public class RDD {

	private Long id;
	private String name;
	private List<Long> parentIDs;
	private Long partitions;
	private Long scopeID;
	private String scopeName;

	/**
	 * @param id
	 * @param parentIDs
	 * @param name
	 * @param scopeID
	 * @param scopeName
	 * @param partitions
	 */
	public RDD(Long id, List<Long> parentIDs, String name, Long scopeID,
			String scopeName, Long partitions) {
		this.id = id;
		this.parentIDs = parentIDs;
		this.name = name;
		this.scopeID = scopeID;
		this.scopeName = scopeName;
		this.partitions = partitions;
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
