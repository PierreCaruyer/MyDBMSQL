package univlille.m1info.abd.memorydb;

import java.util.Map;

import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.SimpleMemoryManager;
import univlille.m1info.abd.schema.RelationSchema;

public class SchemawithMemory extends AbstractSGBD<DefaultRelation> {

	
	/** The size of a page, in number tuples. */
	public static final int PAGE_SIZE = 20;
	public static final int ATTRIBUTE_SIZE = 20;
	public static final MemoryManager mem =  new SimpleMemoryManager(PAGE_SIZE,ATTRIBUTE_SIZE);
	private Map<String, Map<String,Index>> index;
/**
 * 
 * @param schema
 * @return
 */
	
	@Override
	protected DefaultRelation newRelation(RelationSchema schema) {
		return new DefaultRelation(schema, mem);
	}

	
	/**
	 * 
	 * @param nameRelation the name of the relation to which we are looking for an index
	 * @param attributes   a tabular of the name of attributes used in the index
	 * @return the index related to the relation and the attributes
	 */
	
	public Index getIndex (String nameRelation, String [] attributes){
		//TODO
		return null;
	}
	
	
	/**
	 * Add the index for the relation and the attributes
	 * @param nameRelation
	 * @param attributes
	 * @param index
	 */
	public void addIndex	(String nameRelation, String [] attributes, Index index){
			//TODO
	}
	
	/**
	 * Fill the relationname with the tuples (it is the only place where the reset of the memory manager can be used
	 * @param relationname
	 * @param tuples
	 */
	public void FillRelation (String relationname, String[] tuples){
		//TODO
	}
}
	
