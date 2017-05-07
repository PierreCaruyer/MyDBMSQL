package univlille.m1info.abd.memorydb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.SimpleMemoryManager;
import univlille.m1info.abd.schema.RelationSchema;

public class SchemawithMemory extends AbstractSGBD<DefaultRelation> {

	/** The size of a page, in number tuples. */
	public static final int PAGE_SIZE = 20;
	public static final int ATTRIBUTE_SIZE = 20;
	private final MemoryManager mem;
	private Map<String, Map<String, Index>> indexMap = new HashMap<>();

	public SchemawithMemory() {
		mem = new SimpleMemoryManager(PAGE_SIZE, ATTRIBUTE_SIZE);
	}
	
	/**
	 * @param schema
	 * @return
	 */
	@Override
	protected DefaultRelation newRelation(RelationSchema schema) {
		return new DefaultRelation(schema, this);
	}

	/**
	 * 
	 * @param nameRelation
	 *            the name of the relation to which we are looking for an index
	 * @param attributes
	 *            a tabular of the name of attributes used in the index
	 * @return the index related to the relation and the attributes
	 */
	public Index getIndex(String nameRelation, String attribute) {
		Map<String, Index> relationIndexes = indexMap.get(nameRelation);
		if(relationIndexes == null)
			return null;
		return relationIndexes.get(attribute);
	}

	/**
	 * Add the index for the relation and the attributes
	 * 
	 * @param nameRelation
	 * @param attributes
	 * @param index
	 */
	public void addIndex(String nameRelation, String attribute, Index index) {
		Map<String, Index> map = indexMap.get(nameRelation);
		if(map == null) 
			map = new HashMap<>();
		map.put(attribute, index);
		indexMap.put(nameRelation, map);
	}

	/**
	 * Fill the relationname with the tuples (it is the only place where the
	 * reset of the memory manager can be used
	 * 
	 * @param relationname
	 * @param tuples
	 */
	public void FillRelation(String relationName, List<String[]> tuples) {
		mem.resetDiskOperationsCount();
		getRelation(relationName).loadTuples(tuples);
	}
	
	public Index getUniqueIndex(RelationSchema schema) {
		String[] attributes = schema.getSort();
		Index index = null;
		for(String attr : attributes)
			if((index = getIndex(schema.getName(), attr)) != null)
				return index;
		return null;
	}
	
	public MemoryManager getMemoryManager(){
		return mem;
	}
}
