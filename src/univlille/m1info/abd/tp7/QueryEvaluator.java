package univlille.m1info.abd.tp7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.ProjectionOperator;
import univlille.m1info.abd.phys.RenameOperator;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.RenameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.ra.UnaryRAQuery;

public class QueryEvaluator {

	protected SchemawithMemory sgbd;
	protected MemoryManager memoryManager;
	protected RAQuery query;

	public QueryEvaluator(RAQuery query, SchemawithMemory sgbd) {
		this.sgbd = sgbd;
		this.query = query;
		this.memoryManager = sgbd.getMemoryManager();
	}

	public List<String[]> evaluate() {
		List<String[]> tuples = new ArrayList<>();

		try {
			int address;
			PhysicalOperator op = getOperator();
			while((address = op.nextPage()) != -1) {
				Page page = memoryManager.loadPage(address);
				page.switchToReadMode();
				for(String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple())
					tuples.add(tuple);
				memoryManager.releasePage(address, false);
			}
		} catch(NotEnoughMemoryException e) {
			e.printStackTrace();
		}

		return tuples;
	}

	public void result() {
		List<String[]> tuples = evaluate();
		for(String[] tuple : tuples)
			System.out.println(Arrays.toString(tuple));
	}

	/** Creates an operator that allows to (efficiently) execute the given operation on the given database. */
	protected PhysicalOperator getOperator() {
		PhysicalOperator operator = null;
		if(!(query instanceof UnaryRAQuery) && !(query instanceof JoinQuery)) {
			throw new UnsupportedOperationException("Unrecognized query type : " + query.getClass().getName());
		} else	if(query instanceof UnaryRAQuery) {
			RelationNameQuery relationNameQuery = getRelationNameSubQuery((UnaryRAQuery)query);
			//			String relationName = relationNameQuery.getRelationName();
			//			Index relationIndex = lookForIndex(relationName);
			if(query instanceof ProjectionQuery) {
				ProjectionQuery projection = (ProjectionQuery)query;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
				operator = new ProjectionOperator(sequence, memoryManager, projection.getProjectedAttributesNames());
			} else if(query instanceof SelectionQuery) {
				SelectionQuery selection = (SelectionQuery)query;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
				operator = new SelectionOperator(sequence, selection.getAttributeName(), selection.getConstantValue(), selection.getComparisonOperator(), memoryManager);
			} else if(query instanceof RenameQuery) {
				RenameQuery rename = (RenameQuery)query;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
				operator = new RenameOperator(sequence, rename.getOldAttrName(), rename.getNewAttrName(), memoryManager);
			}
		}
		else {
			JoinQuery join = (JoinQuery)query;
			SequentialAccessOnARelationOperator leftSequence, rightSequence;
			RelationNameQuery rightRelationNameQuery = getRightSubQueryName(join), leftRelationNameQuery = getLeftSubQueryName(join);
			//			Index leftIndex = lookForIndex(rightRelationNameQuery.getRelationName());
			//			Index rightIndex = lookForIndex(leftRelationNameQuery.getRelationName());
			rightSequence = getSequentialAccessFromRelationName(sgbd, rightRelationNameQuery.getRelationName());
			leftSequence = getSequentialAccessFromRelationName(sgbd, leftRelationNameQuery.getRelationName());

			operator = new JoinOperator(rightSequence, leftSequence, sgbd.getMemoryManager());
		}
		return operator;
	}

	protected RelationNameQuery getLeftSubQueryName(JoinQuery query) {
		return getJoinSubQueryName(query, true);
	}

	protected RelationNameQuery getRightSubQueryName(JoinQuery query) {
		return getJoinSubQueryName(query, false);
	}

	protected RelationNameQuery getJoinSubQueryName(JoinQuery query, boolean left) {
		RAQuery subQuery;

		subQuery = (left)? query.getLeftSubQuery() : query.getRightSubQuery();

		if(subQuery instanceof RelationNameQuery)
			return (RelationNameQuery)subQuery;
		else if(subQuery instanceof UnaryRAQuery)
			return getRelationNameSubQuery((UnaryRAQuery)subQuery);

		return getJoinSubQueryName((JoinQuery)subQuery, left);
	}

	protected SequentialAccessOnARelationOperator getSequentialAccessFromRelationName(SchemawithMemory sgbd, String relName) {
		DefaultRelation relation;
		SequentialAccessOnARelationOperator sequentialOperator;

		relation = sgbd.getRelation(relName);
		sequentialOperator = new SequentialAccessOnARelationOperator(relation, sgbd.getMemoryManager());

		return sequentialOperator;
	}

	protected RelationNameQuery getRelationNameSubQuery(UnaryRAQuery query) {
		RAQuery subQuery = query.getSubQuery();

		if(subQuery instanceof RelationNameQuery)
			return (RelationNameQuery)subQuery;

		return getRelationNameSubQuery((UnaryRAQuery)subQuery);
	}

	protected Index lookForIndex(String relName) {
		Index ind = null;
		DefaultRelation relation = sgbd.getRelation(relName);
		String[] sorts = relation.getRelationSchema().getSort();
		for(String sort : sorts)
			if((ind = sgbd.getIndex(relName, sort)) != null)
				return ind;
		return null;
	}
}
