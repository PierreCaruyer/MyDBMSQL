package univlille.m1info.abd.phys.index;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.RelationSchema;

public class SelectionWithIndex extends SelectionOperator implements IndexOperator{

	protected DefaultIndex index;
	
	public SelectionWithIndex(IndexOperator operator, String attrName, String value, ComparisonOperator comparator, SchemawithMemory sgbd) {
		super(operator, attrName, value, comparator, sgbd.getMemoryManager());
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
