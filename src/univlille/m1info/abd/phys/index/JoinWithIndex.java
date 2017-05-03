package univlille.m1info.abd.phys.index;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.schema.RelationSchema;

public class JoinWithIndex extends JoinOperator implements IndexOperator{

	
	
	public JoinWithIndex(IndexOperator right, IndexOperator left, SchemawithMemory sgbd) {
		super(right, left, SchemawithMemory.mem);
	}

	protected DefaultIndex index;

	@Override
	public String[] nextTuple() {
		return super.nextTuple();
	}

	@Override
	public RelationSchema resultSchema() {
		return super.resultSchema();
	}

	@Override
	public void reset() {
		super.reset();
	}

	@Override
	public int nextPage() {
		int address = super.nextPage();
		index.createIndex(address);
		return address;
	}

	@Override
	public Index getIndex() {
		return index;
	}
}
