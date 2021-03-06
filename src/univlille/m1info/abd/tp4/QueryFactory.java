package univlille.m1info.abd.tp4;

import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.RenameQuery;
import univlille.m1info.abd.ra.SelectionQuery;

/**
 * Copies input query
 */
public class QueryFactory {

	public static RAQuery copyQuery(RAQuery q) {
		RAQuery copy = null;
		
		if(q instanceof SelectionQuery) {
			SelectionQuery tmp = (SelectionQuery)q;
			copy = new SelectionQuery(tmp.getSubQuery(), tmp.getAttributeName(), tmp.getComparisonOperator(), tmp.getConstantValue());
		}
		else if(q instanceof ProjectionQuery) {
			ProjectionQuery tmp = (ProjectionQuery)q;
			copy = new ProjectionQuery(tmp.getSubQuery(), tmp.getProjectedAttributesNames());
		}
		else if(q instanceof JoinQuery) {
			JoinQuery tmp = (JoinQuery)q;
			copy = new JoinQuery(tmp.getLeftSubQuery(), tmp.getRightSubQuery());
		}
		else if(q instanceof RenameQuery) {
			RenameQuery tmp = (RenameQuery)q;
			copy = new RenameQuery(tmp.getSubQuery(), tmp.getOldAttrName(), tmp.getNewAttrName());
		}
		else { //instance of RelationNameQuery
			RelationNameQuery tmp = (RelationNameQuery)q;
			copy = new RelationNameQuery(tmp.getRelationName());
		}
		
		return copy;
	}
	
	public static RAQuery copyCustomQuery(RAQuery q, RAQuery leftSubQuery, RAQuery rightSubQuery) {
		RAQuery copy = null;
		
		if(q instanceof SelectionQuery) {
			SelectionQuery tmp = (SelectionQuery)q;
			copy = new SelectionQuery(leftSubQuery, tmp.getAttributeName(), tmp.getComparisonOperator(), tmp.getConstantValue());
		}
		else if(q instanceof ProjectionQuery) {
			ProjectionQuery tmp = (ProjectionQuery)q;
			copy = new ProjectionQuery(leftSubQuery, tmp.getProjectedAttributesNames());
		}
		else if(q instanceof JoinQuery) {
			copy = new JoinQuery(leftSubQuery, rightSubQuery);
		}
		else if(q instanceof RenameQuery) {
			RenameQuery tmp = (RenameQuery)q;
			copy = new RenameQuery(leftSubQuery, tmp.getOldAttrName(), tmp.getNewAttrName());
		}
		else { //instance of RelationNameQuery
			RelationNameQuery tmp = (RelationNameQuery)q;
			copy = new RelationNameQuery(tmp.getRelationName());
		}
		
		return copy;
	}
	
	public static SelectionQuery getSelectionWithOldAttribute(SelectionQuery selection, RenameQuery rename) {
		return new SelectionQuery(rename.getSubQuery(), rename.getOldAttrName(), selection.getComparisonOperator(), selection.getConstantValue());
	}
}
