package univlille.m1info.abd.tp7;

import java.util.Arrays;

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
import univlille.m1info.abd.ra.OptimizerVisitorWithMemory;
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

	public QueryEvaluator(SchemawithMemory sgbd, RAQuery query) {
		this.sgbd = sgbd;
		this.query = query;
		this.memoryManager = SchemawithMemory.mem;
	}

	public void evaluate() {
		try {
			PhysicalOperator operator = getOperator(query);
			int pageNb;
			while ((pageNb = operator.nextPage()) != -1) {
				Page page;
				page = memoryManager.loadPage(pageNb);
				page.switchToReadMode();
				for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple())
					System.out.println(Arrays.toString(tuple));
				memoryManager.releasePage(pageNb, false);
			}
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}

	/** Creates an operator that allows to (efficiently) execute the given operation on the given database. */
	protected PhysicalOperator getOperator(RAQuery query) {
		OptimizerVisitorWithMemory visitor = new OptimizerVisitorWithMemory(sgbd);
		query.accept(visitor);
		RAQuery optimQuery = visitor.topQuery();
		
		PhysicalOperator operator = null;
		if(!(optimQuery instanceof UnaryRAQuery) && !(optimQuery instanceof JoinQuery)) {
			throw new UnsupportedOperationException("Unrecognized optimQuery type : " + optimQuery.getClass().getName());
		} else	if(optimQuery instanceof UnaryRAQuery) {
			RelationNameQuery relationNameQuery = getRelationNameSubQuery((UnaryRAQuery)optimQuery);
			String relationName = relationNameQuery.getRelationName();
//			Index relationIndex = lookForIndex(relationName);
			if(optimQuery instanceof ProjectionQuery) {
				ProjectionQuery projection = (ProjectionQuery)optimQuery;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationName);
				operator = new ProjectionOperator(sequence, memoryManager, projection.getProjectedAttributesNames());
			} else if(optimQuery instanceof SelectionQuery) {
				SelectionQuery selection = (SelectionQuery)optimQuery;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationName);
				operator = new SelectionOperator(sequence, selection.getAttributeName(), selection.getConstantValue(), selection.getComparisonOperator(), memoryManager);
			} else if(optimQuery instanceof RenameQuery) {
				RenameQuery rename = (RenameQuery)optimQuery;
				SequentialAccessOnARelationOperator sequence;

				sequence = getSequentialAccessFromRelationName(sgbd, relationName);
				operator = new RenameOperator(sequence, rename.getOldAttrName(), rename.getNewAttrName(), memoryManager);
			}
		}
		else {
			JoinQuery join = (JoinQuery)optimQuery;
			SequentialAccessOnARelationOperator leftSequence, rightSequence;
			RelationNameQuery rightRelationNameQuery = getRightSubQueryName(join), leftRelationNameQuery = getLeftSubQueryName(join);
//			Index leftIndex = lookForIndex(rightRelationNameQuery.getRelationName());
//			Index rightIndex = lookForIndex(leftRelationNameQuery.getRelationName());
			rightSequence = getSequentialAccessFromRelationName(sgbd, rightRelationNameQuery.getRelationName());
			leftSequence = getSequentialAccessFromRelationName(sgbd, leftRelationNameQuery.getRelationName());

			operator = new JoinOperator(rightSequence, leftSequence, SchemawithMemory.mem);
		}
		return operator;
	}

	protected RelationNameQuery getLeftSubQueryName(JoinQuery optimQuery) {
		return getJoinSubQueryName(optimQuery, true);
	}

	protected RelationNameQuery getRightSubQueryName(JoinQuery optimQuery) {
		return getJoinSubQueryName(optimQuery, false);
	}

	protected RelationNameQuery getJoinSubQueryName(JoinQuery query, boolean left) {
		RAQuery subQuery = (left)? query.getLeftSubQuery() : query.getRightSubQuery();

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
		sequentialOperator = new SequentialAccessOnARelationOperator(relation, SchemawithMemory.mem);

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
