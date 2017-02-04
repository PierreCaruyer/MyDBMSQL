package univlille.m1info.abd.tp3;

import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.ProjectionOperator;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.ra.UnaryRAQuery;
import univlille.m1info.abd.tp2.SimpleDBRelation;
import univlille.m1info.abd.tp2.SimpleSGBD;

public class TP3 {
	/** Creates an operator that allows to (efficiently) execute the given operation on the given database. */
	public PhysicalOperator getOperator(RAQuery query, SimpleSGBD sgbd) {
		PhysicalOperator operator;
		RelationNameQuery relationNameQuery;
		SequentialAccessOnARelationOperator sequence;
		
		if(!(query instanceof ProjectionQuery) && !(query instanceof SelectionQuery) && !(query instanceof JoinQuery)) {
			throw new UnsupportedOperationException("Unrecognized query type : " + query.getClass().getName());
		}
		else if(query instanceof ProjectionQuery) {
			ProjectionQuery projection = (ProjectionQuery)query;
			
			relationNameQuery = getRelationNameSubQuery(projection);
			sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
			operator = new ProjectionOperator(sequence, projection.getAttributeNames());
		}
		else if(query instanceof SelectionQuery){ 
			SelectionQuery selection = (SelectionQuery)query;
			
			relationNameQuery = getRelationNameSubQuery(selection);
			sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
			operator = new SelectionOperator(sequence, selection.getAttributeName(), selection.getConstantValue(), selection.getComparisonOperator());
		}
		else {
			JoinQuery join = (JoinQuery)query;
			SequentialAccessOnARelationOperator leftSequence, rightSequence;
			RelationNameQuery rightRelationNameQuery = getRightSubQueryName(join), leftRelationNameQuery = getLeftSubQueryName(join);
			
			rightSequence = getSequentialAccessFromRelationName(sgbd, rightRelationNameQuery.getRelationName());
			leftSequence = getSequentialAccessFromRelationName(sgbd, leftRelationNameQuery.getRelationName());
			
			operator = new JoinOperator(rightSequence, leftSequence);
		}
		return operator;
	}
	
	private RelationNameQuery getLeftSubQueryName(JoinQuery query) {
		return getJoinSubQueryName(query, true);
	}
	
	private RelationNameQuery getRightSubQueryName(JoinQuery query) {
		return getJoinSubQueryName(query, true);
	}
	
	private RelationNameQuery getJoinSubQueryName(JoinQuery query, boolean left) {
		RAQuery subQuery;
		
		subQuery = (left)? query.getLeftSubQuery() : query.getRightSubQuery();
		
		if(subQuery instanceof RelationNameQuery)
			return (RelationNameQuery)subQuery;
		else if(subQuery instanceof UnaryRAQuery)
			return getRelationNameSubQuery((UnaryRAQuery)subQuery);
		else
			return getJoinSubQueryName((JoinQuery)subQuery, left);
	}
	
	private SequentialAccessOnARelationOperator getSequentialAccessFromRelationName(SimpleSGBD sgbd, String relName) {
		SimpleDBRelation relation;
		SequentialAccessOnARelationOperator sequentialOperator;
		
		relation = sgbd.getRelation(relName);
		relation.switchToReadMode();
		sequentialOperator = new SequentialAccessOnARelationOperator(sgbd, relation.getRelationSchema().getName());
		
		return sequentialOperator;
	}
	
	private RelationNameQuery getRelationNameSubQuery(UnaryRAQuery query) {
		RelationNameQuery subQuery = null;
		
		if(!(query.getSubQuery() instanceof RelationNameQuery))
			throw new UnsupportedOperationException("Bad sub query type : " + query.getClass().getName());
		else
			subQuery = (RelationNameQuery)query.getSubQuery();
		
		return subQuery;
	}
}
