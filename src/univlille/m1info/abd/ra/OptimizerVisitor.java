package univlille.m1info.abd.ra;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

import univlille.m1info.abd.tp4.QueryFactory;

/**
 * Tries to optimize query tree by setting selection queries as direct relation name queries parents
 * so that selections are computed before joins
 */
public class OptimizerVisitor implements RAQueryVisitor{

	private Deque<SelectionQuery> selectionCollector = null;
	private Iterator<SelectionQuery> selectionIterator = null;
	private boolean rootJustInitialized = false;
	private RAQuery optimRoot = null;
	private RAQuery lastQuery = null;
	private RAQuery currentOptimQuery = null;

	public OptimizerVisitor() {
		selectionCollector = new ConcurrentLinkedDeque<>();
		optimRoot = null;
		rootJustInitialized = false;
	}

	public void switchToReadMode() {
		selectionIterator = selectionCollector.iterator();
	}

	public SelectionQuery nextQuery() {
		return (selectionIterator.hasNext())? selectionIterator.next() : null;
	}

	/**
	 * Stacks all selection queries to re-use them later on
	 */
	@Override
	public void visit(SelectionQuery q) {
		if(optimRoot == null) { //A selection cannot be the root of the tree, the aim being to get the selection queries 
								//close to the leaves of the tree
			optimRoot = skipSelectionsFrom(q);
			if(optimRoot instanceof RelationNameQuery) {
				
			}
//			optimRoot = QueryFactory.copyQuery(q);
			rootJustInitialized = true;
		}
		RAQuery sub = q.getSubQuery();
		lastQuery = q;
		sub.accept(this);
		if(sub instanceof RenameQuery) {
			RenameQuery rnq = (RenameQuery)sub;
			q.setAttributeName(rnq.getOldAttrName());
		}
		q.getSubQuery().accept(this);
		selectionCollector.add(q);
	}

	@Override
	public void visit(ProjectionQuery q) {
		if(optimRoot == null) {
			optimRoot = QueryFactory.copyQuery(q);
			rootJustInitialized = true;
		}
		lastQuery = q;
		q.getSubQuery().accept(this);
	}

	@Override
	public void visit(JoinQuery q) {
		if(optimRoot == null) {
			optimRoot = QueryFactory.copyQuery(q);
			rootJustInitialized = true;
		}
		lastQuery = q;
		q.getLeftSubQuery().accept(this);
		q.getRightSubQuery().accept(this);
		System.out.println(q);
	}

	@Override
	public void visit(RenameQuery q) {
		if(optimRoot == null) {
			optimRoot = QueryFactory.copyQuery(q);
			rootJustInitialized = true;
		}
		lastQuery = q;
		q.getSubQuery().accept(this);
		System.out.println(q);
	}

	@Override
	public void visit(RelationNameQuery q) {
		if(lastQuery instanceof UnaryRAQuery) {
			SelectionQuery tmp = nextQuery();
			while((tmp = nextQuery()) != null) { //Re-linking selection queries to the leaves of the tree
				currentOptimQuery = QueryFactory.copyQuery(tmp);
				
			}
		}
		else if(lastQuery instanceof RelationNameQuery) {

		}
		else { //JoinQuery

		}
	}
	
	/**
	 * Starts at some node of the tree and skips all selection queries from there
	 * @param currentEntryPoint
	 * @return the first query which is not a selection
	 */
	public static RAQuery skipSelectionsFrom(RAQuery entryPoint) {
		RAQuery currentNode = entryPoint;
		UnaryRAQuery currentUnary = null;
		
		if(currentNode instanceof UnaryRAQuery) {
			currentUnary = (UnaryRAQuery)currentNode;
			while(currentUnary instanceof SelectionQuery) {
				currentNode = currentUnary.getSubQuery();
				if(currentNode instanceof UnaryRAQuery)
					currentUnary = (UnaryRAQuery)currentNode;
			}
		}
		
		return currentNode;
	}
	
	/**
	 * Pushes selection queries down in the query tree
	 * @param root
	 * @return root of the newly optimized query tree
	 */
	public static RAQuery optimizeTree(RAQuery root) {
		RAQuery optimRoot = null, currentNode = null, currentOptimNode = null;

		currentNode = skipSelectionsFrom(root);
		currentOptimNode = QueryFactory.copyQuery(currentNode);
		if(currentOptimNode instanceof JoinQuery) {
			
		}
		else if(currentOptimNode instanceof RelationNameQuery) {
			
		}
		else { //UnaryRAQuery
			if(currentOptimNode instanceof SelectionQuery) {
				
			}
			else if(currentOptimNode instanceof ProjectionQuery) {
				
			}
			else { //RenameQuery
				
			}
		}
		
		return optimRoot;
	}
	
	public RAQuery getOptimizedRoot() {
		return optimRoot;
	}
}
