package univlille.m1info.abd.phys.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.schema.RelationSchema;

public class SequentialAccessWithIndex extends SequentialAccessOnARelationOperator implements IndexOperator{

	protected DefaultIndex index;
	protected SchemawithMemory sgbd;
	protected DefaultRelation rel;
	protected String relationNameIndex;
	protected String attributeNameIndex;
	
	public SequentialAccessWithIndex(DefaultRelation relation, SchemawithMemory sgbd, String relName, String attrName) {
		super(relation, SchemawithMemory.mem);
		this.sgbd = sgbd;
		this.rel = relation;
		relationNameIndex = relName;
		attributeNameIndex = attrName;
	}

	@Override
	public String[] nextTuple() {
		return super.nextTuple();
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		super.reset();
	}

	@Override
	public int nextPage() {
		try{
			Page page = mem.NewPage(schema.getSort().length);
			Page relationPage = null;
			String[] relationTuple = null;
			Index operatorIndex = sgbd.getIndex(relationNameIndex, attributeNameIndex);
			int operatorAddress = -1, pageAddress = page.getAddressPage();
			
			while((operatorAddress = super.nextPage()) != -1) {
				relationPage = mem.loadPage(operatorAddress);
				relationPage.switchToReadMode();
				
				while((relationTuple = relationPage.nextTuple()) != null) {
					List<Integer> addresses = operatorIndex.getListofAddresses(relationTuple);
					List<Integer> tableAddresses = new ArrayList<>();
					if(addresses == null)
						continue;
					for(Integer addr : addresses) {
						page.AddTuple(relationTuple);
						tableAddresses.add(new Integer(pageAddress));
						if(page.isFull()) {
							index.updateIndex(Arrays.toString(relationTuple), tableAddresses);
							break;
						}
					}
				}
			}
			
			mem.releasePage(page.getAddressPage(), false);
			return pageAddress;
		}catch(NotEnoughMemoryException e) {
			return -1;
		}
	}

	@Override
	public Index getIndex() {
		return index;
	}

}
