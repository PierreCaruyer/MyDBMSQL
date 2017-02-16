package univlille.m1info.abd.ra;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Visitor implements RAQueryVisitor{

	private Deque<RAQuery> selectionCollector;
	private Deque<RAQuery> queryTree;
	private Iterator<RAQuery> iterator;
	
	public Visitor() {
		queryTree = new ConcurrentLinkedDeque<>();
		selectionCollector = new ConcurrentLinkedDeque<>();
		iterator = null;
	}
	
	@Override
	public void switchToReadMode() {
		iterator = queryTree.iterator();
	}
	
	@Override
	public RAQuery nextQuery() {
		return (iterator.hasNext())? iterator.next() : null;
	}
	
	@Override
	public void visit(SelectionQuery q) {
		q.getSubQuery().accept(this);
		RAQuery sub = q.getSubQuery();
		RenameQuery rnq = null;
		if(sub instanceof RenameQuery) {
			rnq = (RenameQuery)sub;
			q.setAttributeName(rnq.getOldAttrName());
		}
		rnq = null;
		selectionCollector.add(q);
		queryTree.push(q);
	}

	@Override
	public void visit(ProjectionQuery q) {
		q.getSubQuery().accept(this);
		queryTree.push(q);
	} 

	@Override
	public void visit(JoinQuery q) {
		q.getLeftSubQuery().accept(this);
		q.getRightSubQuery().accept(this);
		queryTree.push(q);
	}

	@Override
	public void visit(RenameQuery q) {
		q.getSubQuery().accept(this);
		queryTree.push(q);
	}

	@Override
	public void visit(RelationNameQuery q) {
		queryTree.push(q);
	}
}
