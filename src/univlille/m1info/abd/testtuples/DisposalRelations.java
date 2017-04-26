package univlille.m1info.abd.testtuples;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.ProjectionOperator;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.phys.SimpleMemoryManager;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.tp6.TestTP6;

public class DisposalRelations {
	
	private MemoryManager mem;
	
	public DisposalRelations(MemoryManager mem) {
		this.mem = mem;
	}
	
	/**
	 * Loads a Short table
	 */
	public PhysicalOperator getRightLoadedTable() {
		RelationSchema schema = new DefaultRelationSchema("RELONE", new String[] { "attrA", "attrB", "attrC" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		tuples.add(new String[] { "a5", "b1", "c3" });
		tuples.add(new String[] { "a1", "b4", "c6" });
		tuples.add(new String[] { "a2", "b5", "c2" });
		tuples.add(new String[] { "a3", "b8", "c7" });

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	/**
	 * Loads a Short table
	 */
	public PhysicalOperator getLeftLoadedTable() {
		RelationSchema schema = new DefaultRelationSchema("RELTWO", new String[] { "attrE", "attrD", "attrA" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		tuples.add(new String[] { "e4", "d1", "a5" });
		tuples.add(new String[] { "e6", "d4", "a4" });
		tuples.add(new String[] { "e9", "d5", "a3" });
		tuples.add(new String[] { "e6", "d3", "a2" });

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	/**
	 * Loads a table w/ many tuples to test memory allocation and free
	 * mecanisms' correctness
	 */
	public PhysicalOperator getLongRightTable() {
		RelationSchema schema = new DefaultRelationSchema("RELLONGR", new String[] { "attrA", "attrB", "attrC" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < TestTP6.REPEAT; i++) {
			tuples.add(new String[] { "a5", "b1", "c3" });
			tuples.add(new String[] { "a1", "b4", "c6" });
			tuples.add(new String[] { "a2", "b5", "c2" });
			tuples.add(new String[] { "a3", "b8", "c7" });
		}

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	public PhysicalOperator getLongLeftTable() {
		RelationSchema schema = new DefaultRelationSchema("RELLONGR", new String[] { "attrE", "attrD", "attrA" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < TestTP6.REPEAT; i++) {
			tuples.add(new String[] { "e4", "d1", "a5" });
			tuples.add(new String[] { "e6", "d4", "a4" });
			tuples.add(new String[] { "e9", "d5", "a3" });
			tuples.add(new String[] { "e6", "d3", "a2" });
		}

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	// Selection operator w/ few tuples
	public PhysicalOperator getShortSelectionOperator() {
		return new SelectionOperator(getRightLoadedTable(), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}

	// Selection operator w/ more tuples
	public PhysicalOperator getLongSelectionOperator() {
		return new SelectionOperator(getLongRightTable(), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}

	// Projection operator w/ few tuples
	public PhysicalOperator getShortProjectionOperator() {
		return new ProjectionOperator(getRightLoadedTable(), mem, new String[] { "attrA", "attrC" });
	}

	// Projection operator w/ more tuples
	public PhysicalOperator getLongProjectionOperator() {
		return new ProjectionOperator(getLongRightTable(), mem, new String[] { "attrA", "attrC" });
	}
	
	public PhysicalOperator getProjectionOnBAndCAttributes() {
		return new ProjectionOperator(getLongRightTable(), mem, new String[] { "attrB", "attrC"});
	}

	public PhysicalOperator getJoinOperator() {
		return new JoinOperator(getRightLoadedTable(), getLeftLoadedTable(), mem);
	}

	public PhysicalOperator getLongJoinOperator() {
		return new JoinOperator(getLongRightTable(), getLongLeftTable(), mem);
	}
	
	public SequentialAccessOnARelationOperator getSimpleModTable() {
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		MemoryManager mem = new SimpleMemoryManager(100, 2);
		DefaultRelation rel = new DefaultRelation(schema, mem);

		ArrayList<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <= 9; i++) {
			tuples.add(new String[] { "a" + (i % 3), "b" + i });
		}

		rel.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(rel, mem);
	}
	
	public SequentialAccessOnARelationOperator getLeftModTable() {
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		DefaultRelation rel = new DefaultRelation(schema, mem);

		List<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <= 9; i++) {
			tuples.add(new String[] { "a" + (i % 3), "b" + i });
		}

		rel.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(rel, mem);
	}
	
	public SequentialAccessOnARelationOperator getRightModTable() {
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rc");
		DefaultRelation rel = new DefaultRelation(schema, mem);

		List<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <= 9; i++) {
			tuples.add(new String[] { "a" + (i % 3), "c" + i });
		}

		rel.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(rel, mem);
	}
	
	public List<String[]> getExpectedResultJoinTuples() {
		List<String[]> resultArray = new ArrayList<>();
		
		resultArray.add(new String[] { "a1", "b1", "c1" });
		resultArray.add(new String[] { "a1", "b1", "c4" });
		resultArray.add(new String[] { "a1", "b1", "c7" });
		resultArray.add(new String[] { "a2", "b2", "c2" });
		resultArray.add(new String[] { "a2", "b2", "c5" });
		resultArray.add(new String[] { "a2", "b2", "c8" });
		resultArray.add(new String[] { "a0", "b3", "c3" });
		resultArray.add(new String[] { "a0", "b3", "c6" });
		resultArray.add(new String[] { "a0", "b3", "c9" });
		resultArray.add(new String[] { "a1", "b4", "c1" });
		resultArray.add(new String[] { "a1", "b4", "c4" });
		resultArray.add(new String[] { "a1", "b4", "c7" });
		resultArray.add(new String[] { "a2", "b5", "c2" });
		resultArray.add(new String[] { "a2", "b5", "c5" });
		resultArray.add(new String[] { "a2", "b5", "c8" });
		resultArray.add(new String[] { "a0", "b6", "c3" });
		resultArray.add(new String[] { "a0", "b6", "c6" });
		resultArray.add(new String[] { "a0", "b6", "c9" });
		resultArray.add(new String[] { "a1", "b7", "c1" });
		resultArray.add(new String[] { "a1", "b7", "c4" });
		resultArray.add(new String[] { "a1", "b7", "c7" });
		resultArray.add(new String[] { "a2", "b8", "c2" });
		resultArray.add(new String[] { "a2", "b8", "c5" });
		resultArray.add(new String[] { "a2", "b8", "c8" });
		resultArray.add(new String[] { "a0", "b9", "c3" });
		resultArray.add(new String[] { "a0", "b9", "c6" });
		resultArray.add(new String[] { "a0", "b9", "c9" });
		
		return resultArray;
	}
}
