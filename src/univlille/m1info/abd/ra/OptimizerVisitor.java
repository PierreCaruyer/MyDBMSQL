package univlille.m1info.abd.ra;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;
import univlille.m1info.abd.tp4.QueryFactory;

public class OptimizerVisitor implements RAQueryVisitor {
	private SimpleSGBD sgbd;
	private ArrayList<SelectionQuery> queue;
	private ArrayDeque<RAQuery> routes;

	public OptimizerVisitor(SimpleSGBD sgbd) {
		super();
		this.sgbd = sgbd;
		queue = new ArrayList<SelectionQuery>();
		routes = new ArrayDeque<RAQuery>();
	}

	@Override
	public void visit(SelectionQuery q) {
		System.out.println("* SEL");
		System.out.println(q.toString());
		
		if ( !(q.getSubQuery() instanceof RelationNameQuery) ) queue.add(q);
		
		q.getSubQuery().accept(this);
	}

	@Override
	public void visit(ProjectionQuery q) {
		System.out.println("* PROJ");
		System.out.println(q.toString());
		
		RAQuery subQuery = q.getSubQuery();
		if ( subQuery instanceof SelectionQuery ) {
			SelectionQuery select = (SelectionQuery) subQuery;
			queue.add((SelectionQuery) select);
			RAQuery sub = select.getSubQuery();
			ProjectionQuery ret = (ProjectionQuery) QueryFactory.copyCustomQuery(q, sub, null);
			
			addRoute(ret);
			ret.getSubQuery().accept(this);
		} else {
			System.out.println("proj q = "+q);
		
			addRoute(q);
			q.getSubQuery().accept(this);
		}
	}
	
	@Override
	public void visit(JoinQuery q) {
		System.out.println("* JOIN");
		System.out.println(q.toString());
		
		RAQuery leftQuery = q.getLeftSubQuery();
		RAQuery rightQuery = q.getRightSubQuery();
		if ( leftQuery instanceof SelectionQuery ) {
			SelectionQuery select = (SelectionQuery) leftQuery;
			queue.add((SelectionQuery) select);
			leftQuery = QueryFactory.copyCustomQuery(leftQuery, select.getSubQuery(), null);
		}
		if ( rightQuery instanceof SelectionQuery ) {
			SelectionQuery select = (SelectionQuery) rightQuery;
			queue.add((SelectionQuery) select);
			rightQuery = QueryFactory.copyCustomQuery(rightQuery, select.getSubQuery(), null);
		}
		RAQuery query = QueryFactory.copyCustomQuery(q, leftQuery, rightQuery);
		
		addRoute(query);
		leftQuery.accept(this);
		rightQuery.accept(this);
	}

	@Override
	public void visit(RenameQuery q) {
		System.out.println("RENA");
		//q.getSubQuery().accept(this);
	}

	@Override
	public void visit(RelationNameQuery q) {
		System.out.println("REL");
		if ( queue.size() == 0 ) {
			addRoute(q);
		}
		else { 
			SimpleDBRelation relation = sgbd.getRelation(q.getRelationName());
			RelationSchema schema = relation.getRelationSchema();
			String[] sorts = schema.getSort();
			
			boolean selectWaiting = false;
			String selectAttr;
			for ( int i=0; i < queue.size(); i++ ) {
				SelectionQuery select = queue.get(i);
				selectAttr = select.getAttributeName();
				
				if ( Arrays.asList(sorts).contains(selectAttr) ) {
					selectWaiting = true;
					
					SelectionQuery sel = new SelectionQuery(
						q, 
						selectAttr, 
						select.getComparisonOperator(), 
						select.getConstantValue());
					addRoute(sel);
					queue.remove(i);
					sel.accept(this);
					break;
				}
			}
			if ( !selectWaiting ) addRoute(q);
		}
	}
	
	private void addRoute(RAQuery q) {
		RAQuery query = QueryFactory.copyQuery(q);
		routes.add(query);
	}

	public RAQuery topQuery() {
		Stack<RAQuery> stack = new Stack<RAQuery>();
		
		RAQuery res = null;
		while ( !routes.isEmpty() ) {
			RAQuery query = routes.pollLast();
			
			if ( query instanceof RelationNameQuery ) {
				stack.push(query);
			} else if ( query instanceof SelectionQuery ) {
				res = QueryFactory.copyCustomQuery(query, stack.pop(), null);
				stack.push(res);
			} else if ( query instanceof ProjectionQuery ) {
				res = QueryFactory.copyCustomQuery(query, stack.pop(), null);
			} else if ( query instanceof JoinQuery ) {
				res = QueryFactory.copyCustomQuery(query, stack.pop(), stack.pop());
				stack.push(res);
			} else if ( query instanceof RenameQuery ) {
				// TODO
			} 
		}
		System.out.println("RES => " + res);
		
		return res;
	}
}