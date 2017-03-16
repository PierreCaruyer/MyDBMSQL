package univlille.m1info.abd.tp3;

import univlille.m1info.abd.phys.JoinOperator;
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
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

public class TP3 {
	/** Creates an operator that allows to (efficiently) execute the given operation on the given database. */
	public PhysicalOperator getOperator(RAQuery query, SimpleSGBD sgbd) {
		PhysicalOperator operator;
		if(!(query instanceof ProjectionQuery) && !(query instanceof SelectionQuery) && !(query instanceof JoinQuery) && !(query instanceof RenameQuery)) {
			throw new UnsupportedOperationException("Unrecognized query type : " + query.getClass().getName());
		}
		else if(query instanceof ProjectionQuery) {
			ProjectionQuery projection = (ProjectionQuery)query;
			SequentialAccessOnARelationOperator sequence;
			RelationNameQuery relationNameQuery;
			
			relationNameQuery = getRelationNameSubQuery(projection);
			sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
			operator = new ProjectionOperator(sequence, projection.getProjectedAttributesNames());
		}
		else if(query instanceof SelectionQuery) {
			SelectionQuery selection = (SelectionQuery)query;
			SequentialAccessOnARelationOperator sequence;
			RelationNameQuery relationNameQuery;
			
			relationNameQuery = getRelationNameSubQuery(selection);
			sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
			operator = new SelectionOperator(sequence, selection.getAttributeName(), selection.getConstantValue(), selection.getComparisonOperator());
		}
		else if(query instanceof RenameQuery) {
			RenameQuery rename = (RenameQuery)query;
			SequentialAccessOnARelationOperator sequence;
			RelationNameQuery relationNameQuery;
			
			relationNameQuery = getRelationNameSubQuery(rename);
			sequence = getSequentialAccessFromRelationName(sgbd, relationNameQuery.getRelationName());
			operator = new RenameOperator(sequence, rename.getOldAttrName(), rename.getNewAttrName());
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
		return getJoinSubQueryName(query, false);
	}
	
	private RelationNameQuery getJoinSubQueryName(JoinQuery query, boolean left) {
		RAQuery subQuery;
		
		subQuery = (left)? query.getLeftSubQuery() : query.getRightSubQuery();
		
		if(subQuery instanceof RelationNameQuery)
			return (RelationNameQuery)subQuery;
		else if(subQuery instanceof UnaryRAQuery)
			return getRelationNameSubQuery((UnaryRAQuery)subQuery);
		
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
		RAQuery subQuery = query.getSubQuery();
		
		if(subQuery instanceof RelationNameQuery)
			return (RelationNameQuery)subQuery;
		
		return getRelationNameSubQuery((UnaryRAQuery)subQuery);
	}
}
