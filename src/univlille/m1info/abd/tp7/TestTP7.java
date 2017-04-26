package univlille.m1info.abd.tp7;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.tp6.TestTP6;

public class TestTP7 {
	
	private MemoryManager mem;
	private SchemawithMemory memSchema;
	private String testedRelName = "REL";
	private String testedColName = "attrA";
	
	@Before
	public void setUp() {
		memSchema = new SchemawithMemory();
	}
	
	@Test
	public void testMultiplePageIndex() {
		getLongRightTable();
		Index index = new DefaultIndex(testedRelName, testedColName, memSchema);
		memSchema.addIndex(testedRelName, testedColName, index);
		assertTrue(moreThanOneAddress(index.getListofAddresses(new String[] { "a5", "b1", "c3" })));
	}

	private boolean moreThanOneAddress(Iterator<Integer> it) {
		int count = 0;
		for(;it.hasNext();) {
			it.next();
			count++;
		}
		return count > 1;
	}
	/**
	 * Loads a table w/ many tuples to test memory allocation and free
	 * mecanisms' correctness
	 */
	private PhysicalOperator getLongRightTable() {
		memSchema.createRelation(testedRelName, new String[] { testedColName, "attrB", "attrC" });
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < TestTP6.REPEAT; i++) {
			tuples.add(new String[] { "a5", "b1", "c3" });
			tuples.add(new String[] { "a1", "b4", "c6" });
			tuples.add(new String[] { "a2", "b5", "c2" });
			tuples.add(new String[] { "a3", "b8", "c7" });
		}

		memSchema.FillRelation(testedRelName, tuples);

		return new SequentialAccessOnARelationOperator(memSchema.getRelation(testedRelName), mem);
	}
}
