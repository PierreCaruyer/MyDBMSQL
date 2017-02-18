package univlille.m1info.abd.ra;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Visitor implements RAQueryVisitor{

	private Deque<SelectionQuery> selectionCollector;
	private Iterator<SelectionQuery> selectionIterator;
	private Deque<RAQuery> queryTree;
	private Iterator<RAQuery> queryIterator;
	
	public Visitor() {
		queryTree = new ConcurrentLinkedDeque<>();
		selectionCollector = new ConcurrentLinkedDeque<>();
	}
	
	public void switchToReadMode() {
		queryIterator = queryTree.iterator();
		selectionIterator = selectionCollector.iterator();
	}
	
	public RAQuery nextQuery() {
		return (queryIterator.hasNext())? queryIterator.next() : null;
	}
	
	public SelectionQuery nextSelection() {
		return (selectionIterator.hasNext())? selectionIterator.next() : null;
	}
	
	@Override
	public void visit(SelectionQuery q) {
		q.getSubQuery().accept(this);
		RAQuery sub = q.getSubQuery();
		if(sub instanceof RenameQuery) {
			RenameQuery rnq = (RenameQuery)sub;
			q.setAttributeName(rnq.getOldAttrName());
		}
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
