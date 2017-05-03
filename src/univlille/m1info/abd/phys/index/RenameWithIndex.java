package univlille.m1info.abd.phys.index;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.RenameOperator;
import univlille.m1info.abd.schema.RelationSchema;

public class RenameWithIndex extends RenameOperator implements IndexOperator{

	protected DefaultIndex index;
	
	public RenameWithIndex(IndexOperator operator, String attrName, String renamedAttribute, SchemawithMemory sgbd) {
		super(operator, attrName, renamedAttribute, SchemawithMemory.mem);
	}

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
