package univlille.m1info.abd.ra;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;
import univlille.m1info.abd.tp4.QueryFactory;

/**
 * Tries to optimize query tree by setting selection queries as direct relation name queries parents
 * so that selections are computed before joins
 */
public class OptimizerVisitor implements RAQueryVisitor{

	private SimpleSGBD sgbd = null;
	private Deque<SelectionQuery> selectionCollector = null;
	private RAQuery topQuery = null, tmpQuery = null;
	private SelectionQuery firstQuery = null;

	public OptimizerVisitor(SimpleSGBD sgbd) {
		selectionCollector = new ConcurrentLinkedDeque<>();
		this.sgbd = sgbd;
	}

	private void switchToReadMode() {
		firstQuery = selectionCollector.peekFirst();
	}
	
	private SelectionQuery nextQuery() {
		SelectionQuery temporary = selectionCollector.peekFirst();
		
		if(temporary == null || temporary == firstQuery)
			return null;
		selectionCollector.removeFirst();
		selectionCollector.addLast(temporary);
		
		return temporary;
	}
	
	@Override
	public void visit(SelectionQuery q) {
		skipSelectionsFrom(q).accept(this);
	}

	@Override
	public void visit(ProjectionQuery q) {
		q.getSubQuery().accept(this);
		if(tmpQuery != null)
			tmpQuery = topQuery;
		topQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
	}

	@Override
	public void visit(JoinQuery q) {
		q.getLeftSubQuery().accept(this);
		q.getRightSubQuery().accept(this);

		topQuery = QueryFactory.copyCustomQuery(q, topQuery, tmpQuery);
	}

	@Override
	public void visit(RenameQuery q) {
		q.getSubQuery().accept(this);
		if(tmpQuery != null)
			tmpQuery = topQuery;
		topQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
	}

	@Override
	public void visit(RelationNameQuery q) {
		SelectionQuery tmp = null;
		SimpleDBRelation relation = sgbd.getRelation(q.getRelationName());
		List<SelectionQuery> selectionList = new ArrayList<>();

		switchToReadMode();
		while((tmp = nextQuery()) != null)
			selectionList.add(tmp);

		if(selectionList.isEmpty()) {
			topQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
		}
		else {
			topQuery = QueryFactory.copyCustomQuery(selectionList.get(0), q, null);

			for(int i = 1; i < selectionList.size(); i++){ //Filter selection queries
				SelectionQuery selection = selectionList.get(i);
				String[] sorts = relation.getRelationSchema().getSort();
				if(arrayContains(sorts, selection.getAttributeName())) {
					topQuery = QueryFactory.copyCustomQuery(selectionList.get(i), topQuery, null);
				}
			}
		}
	}

	/**
	 * Starts at some node of the tree and skips all selection queries from there
	 * @param currentEntryPoint
	 * @return the first query which is not a selection
	 */
	private RAQuery skipSelectionsFrom(SelectionQuery entryPoint) {
		RAQuery currentSub = null;
		UnaryRAQuery currentUnary = entryPoint;

		while(currentUnary instanceof SelectionQuery) {
			selectionCollector.addLast((SelectionQuery)currentUnary);
			currentSub = currentUnary.getSubQuery();
			if(currentSub instanceof UnaryRAQuery)
				currentUnary = (UnaryRAQuery)currentSub;
			else break;
		}

		return currentSub;
	}

	private boolean arrayContains(String[] array, String str) {
		for(String s : array)
			if(s.equals(str))
				return true;
		return false;
	}

	public RAQuery topQuery() {
		return topQuery;
	}
}
