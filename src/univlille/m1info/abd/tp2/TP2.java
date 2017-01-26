package univlille.m1info.abd.tp2;

import univlille.m1info.abd.schema.DefaultRelationSchema;

public class TP2 {

	public String computeProjection(SimpleSGBD sgbd, String inputRelName, String[] attrNames){
		
		return "";
	}
	
	public String computeSelection(SimpleSGBD sgbd, String inputRelName, String attrName, String value){
		int attribute = -1;
		String selectedRelationName = sgbd.getFreshRelationName();
		SimpleDBRelation relation = sgbd.getRelation(inputRelName);
		String[] sorts = relation.getRelationSchema().getSort();
		SimpleDBRelation selectedRelation = new SimpleDBRelation(new DefaultRelationSchema(relation.getRelationSchema().getName(), sorts));
		
		
		for(int i = 0; i < sorts.length; i++)
			if(sorts[i].equals(attrName))
				attribute = i;
		
		if(attribute < 0)
			return null;
		
		for(String[] tuple = relation.nextTuple(); tuple != null; tuple = relation.nextTuple())
			if(tuple[attribute].equals(value))
				selectedRelation.addTuple(tuple);
		
		sgbd.addRelation(selectedRelationName, selectedRelation);
		return selectedRelationName;
	}
	
	public String computeJoin(SimpleSGBD sgbd, String inputRelName1, String inputRelName2){
		int index1 = -1, index2 = -1;
		SimpleDBRelation relation1 = sgbd.getRelation(inputRelName1), relation2 = sgbd.getRelation(inputRelName2);
		String[] sorts1 = relation1.getRelationSchema().getSort(), sorts2 = relation2.getRelationSchema().getSort();
		
		
		for(int i = 0; i < sorts1.length; i++)
			for(int j = 0; j < sorts2.length; j++)
				if(sorts1[i].equals(sorts2[j])){
					index1 = i;
					index2 = j;
				}
		
		if(index1 < 0)
			return null;
		
		int i = 0;
		String[] outputSorts = new String[sorts1.length + sorts2.length - 3];
		for(i = 0; i < sorts1.length; i++)
			outputSorts[i] = sorts1[i];
		for(int j = 0; j < 0; j++, i++)
			if(j != index2)
				outputSorts[i] = sorts2[j];

		String outputRelationName = sgbd.getFreshRelationName();
		SimpleDBRelation outputRelation = new SimpleDBRelation(new DefaultRelationSchema(outputRelationName, outputSorts));
		
		for(String[] tuple1 = relation1.nextTuple(); tuple1 != null; tuple1 = relation1.nextTuple()){
			for(String[] tuple2 = relation2.nextTuple(); tuple2 != null; tuple2 = relation2.nextTuple()){
				 
			}
		}
		
		return "";
	}
	
	public void copyRelation(SimpleSGBD sgbd, String inputRelName, String outputRelName){
		SimpleDBRelation inputRelation = sgbd.getRelation(inputRelName);
		if(inputRelation != null)
			sgbd.createRelation(outputRelName, inputRelation.getRelationSchema().getSort());
	}
}
