package univlille.m1info.abd.ra;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
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
	private Iterator<SelectionQuery> selectionIterator = null;
	private RAQuery topQuery = null;
	private RAQuery leftQuery = null, rightQuery = null; //Used to describe join queries' subqueries
	private boolean leftQueryEnabled = false, rightQueryEnabled = false;

	public OptimizerVisitor(SimpleSGBD sgbd) {
		selectionCollector = new ConcurrentLinkedDeque<>();
		this.sgbd = sgbd;
	}

	private void switchToReadMode() {
		selectionIterator = selectionCollector.iterator();
	}

	private SelectionQuery nextQuery() {
		return (selectionIterator.hasNext())? selectionIterator.next() : null;
	}

	/**
	 * Stacks all selection queries to re-use them later on
	 */
	@Override
	public void visit(SelectionQuery q) {
		System.out.println(q);
		skipSelectionsFrom(q).accept(this);
	}

	@Override
	public void visit(ProjectionQuery q) {
		System.out.println(q);
		q.getSubQuery().accept(this);
		if(rightQueryEnabled) {
			rightQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
			rightQueryEnabled = false;
		}
		else if(leftQueryEnabled) {
			leftQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
			leftQueryEnabled = false;
		}
		else {
			topQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
		}
	}

	@Override
	public void visit(JoinQuery q) {
		System.out.println(q);
		//leftQuery will take the role of topQuery
		leftQueryEnabled = true;
		q.getLeftSubQuery().accept(this);
		
		//rightQuery will take the role of topQuery
		rightQueryEnabled = true;
		q.getRightSubQuery().accept(this);
		
		topQuery = QueryFactory.copyCustomQuery(q, leftQuery, rightQuery);
	}

	@Override
	public void visit(RenameQuery q) {
		System.out.println(q);
		q.getSubQuery().accept(this);
		if(rightQueryEnabled) {
			rightQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
			rightQueryEnabled = false;
		}
		else if(leftQueryEnabled) {
			leftQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
			leftQueryEnabled = false;
		}
		else {
			topQuery = QueryFactory.copyCustomQuery(q, topQuery, null);
		}
	}

	@Override
	public void visit(RelationNameQuery q) {
		System.out.println(q);
		SelectionQuery tmp;
		SimpleDBRelation relation = sgbd.getRelation(q.getRelationName());
		List<SelectionQuery> selectionList = new ArrayList<>();

		switchToReadMode();
		while((tmp = nextQuery()) != null)
			selectionList.add(tmp);

		topQuery = QueryFactory.copyCustomQuery(selectionList.get(0), q, null);

		for(int i = 1; i < selectionList.size(); i++){ //Filter selection queries
			SelectionQuery selection = selectionList.get(i);
			String[] sorts = relation.getRelationSchema().getSort();
			if(arrayContains(sorts, selection.getAttributeName())) {
				topQuery = QueryFactory.copyCustomQuery(selectionList.get(i), topQuery, null);
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
			selectionCollector.push((SelectionQuery)currentUnary);
			currentSub = currentUnary.getSubQuery();
			if(currentSub instanceof UnaryRAQuery)
				currentUnary = (UnaryRAQuery)currentSub;
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
