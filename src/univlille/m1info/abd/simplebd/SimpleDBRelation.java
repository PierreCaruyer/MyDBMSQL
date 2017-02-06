package univlille.m1info.abd.simplebd;
 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import univlille.m1info.abd.schema.RelationSchema;

/** A naive implementation of a database table.
 * 
 * Can be in read mode or in write mode.
 * In read mode, allows to iterate over all the tuples, but cannot be modified.
 * In write mode, allows to append with new tuples, but cannot be traversed.
 * Is in write mode on creation;
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 25 janv. 2017
 */
public class SimpleDBRelation {
	
	private final RelationSchema schema;
	enum Mode {READ, WRITE};
	private ArrayList<String[]> tuples;
	
	private Iterator<String[]> readIterator;
	
	/** Creates an empty table in write mode.
	 * 
	 * @param arity
	 */
	public SimpleDBRelation (RelationSchema schema) {
		this.schema = schema;
		this.tuples = new ArrayList<>();
	}
	
	/** Initialises the iteration over the tuples. */
	public void switchToReadMode () {
		this.readIterator = tuples.iterator();
	}
	
	public void switchToWriteMode() {
		this.readIterator = null;
	}
		
	public Mode getMode() {
		if (readIterator != null)
			return Mode.READ;
		return Mode.WRITE;
	}
	
	public RelationSchema getRelationSchema() {
		return this.schema;
	}
	
	
	/** Returns the next tuple of the relation, or null if all the tuples have been visited. 
	 * Error if in write mode.
	 */
	public String[] nextTuple () {
		if (getMode() == Mode.WRITE)
			throw new IllegalStateException("Cannot iterate over the tuples while in write mode.");
		
		if (! readIterator.hasNext())
			return null;
		return readIterator.next();
	} 
	
	/** Adds a tuple to the relation.
	 * Error if in read mode.
	 * Error if the tuple does not have the correct arity.
	 * 
	 * @param tuple
	 */
	public void addTuple (String[] tuple) {
		if (getMode() == Mode.READ)
			throw new IllegalStateException("Cannot modify the table while in read mode.");
		if (tuple.length != schema.getSort().length)
			throw new IllegalArgumentException("Incompatible arity. Expected " + schema.getSort().length + " found " + tuple.length);
		
		tuples.add(Arrays.copyOf(tuple, tuple.length));
	}
}
