package univlille.m1info.abd.tp2;

import java.util.HashMap;
import java.util.Map;

import univlille.m1info.abd.schema.DefaultRelationSchema;

public class TP2 {

	private boolean contains(String str, String[] array){
		for(String string : array)
			if(str.equals(string))
				return true;
		return false;
	}
	
	public String computeProjection(SimpleSGBD sgbd, String inputRelName, String[] attrNames){
		int count = 0;
		String projectionName = sgbd.getFreshRelationName();
		SimpleDBRelation relation = sgbd.getRelation(inputRelName), projection;
		String[] sorts = relation.getRelationSchema().getSort(), projectionSorts = new String[attrNames.length];
		Map<Integer, Integer> relationMapping = new HashMap<>(); 
		
		for(int i = 0; i < sorts.length; i++){
			if(contains(sorts[i], attrNames)){
				projectionSorts[i] = sorts[i];
				relationMapping.put(count, i);
				count++;
			}
		}
		
		projection = new SimpleDBRelation(new DefaultRelationSchema(projectionName, projectionSorts));
		relation.switchToReadMode();
		
		for(String[] tuple = relation.nextTuple(); tuple != null; tuple = relation.nextTuple()){
			String[] projectionTuple = new String[attrNames.length];
			for(int i = 0; i < count; i++)
				projectionTuple[i] = tuple[relationMapping.get(i)];
			projection.addTuple(projectionTuple);
		}
		
		sgbd.addRelation(projectionName, projection);
		
		return projectionName;
	}
	
	public String computeSelection(SimpleSGBD sgbd, String inputRelName, String attrName, String value){
		int attribute = -1;
		String selectedRelationName = sgbd.getFreshRelationName();
		SimpleDBRelation relation = sgbd.getRelation(inputRelName);
		String[] sorts = relation.getRelationSchema().getSort();
		SimpleDBRelation selectedRelation = new SimpleDBRelation(new DefaultRelationSchema(selectedRelationName, sorts));
		
		for(int i = 0; i < sorts.length; i++)
			if(sorts[i].equals(attrName))
				attribute = i;
		
		if(attribute < 0)
			return null;
		
		relation.switchToReadMode();
		
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
		
		/*
		 * Récupère l'indice de l'attribut commun sur lequel on fera la jointure.
		 */
		for(int i = 0; i < sorts1.length; i++)
			for(int j = 0; j < sorts2.length; j++)
				if(sorts1[i].equals(sorts2[j])){
					index1 = i;
					index2 = j;
				}
		
		if(index1 < 0)
			return null;
		
		/*
		 * Associe à la relation de sortie les attributs des relations en entrée.
		 */
		int i = 0, j = 0;
		String[] outputSorts = new String[sorts1.length + sorts2.length - 3];
		for(i = 0; i < sorts1.length; i++)
			outputSorts[i] = sorts1[i];
		for(j = 0; j < 0; j++, i++)
			if(j != index2)
				outputSorts[i] = sorts2[j];

		String outputRelationName = sgbd.getFreshRelationName();
		SimpleDBRelation outputRelation = new SimpleDBRelation(new DefaultRelationSchema(outputRelationName, outputSorts));
		
		/*
		 * Copie des tuples dans la relation de sortie.
		 */
		for(String[] tuple1 = relation1.nextTuple(); tuple1 != null; tuple1 = relation1.nextTuple()){
			for(String[] tuple2 = relation2.nextTuple(); tuple2 != null; tuple2 = relation2.nextTuple()){
				 String[] newTuple = new String[tuple1.length + tuple2.length - 3];
				 for(i = 0; i < tuple1.length; i++)
					 newTuple[i] = tuple1[i];
				 for(j = 0; j < tuple2.length; j++, i++)
					 newTuple[i] = tuple2[j];
				 outputRelation.addTuple(newTuple);
			}
		}
		
		sgbd.addRelation(outputRelationName, outputRelation);
		
		return outputRelationName;
	}
	
	public void copyRelation(SimpleSGBD sgbd, String inputRelName, String outputRelName){
		SimpleDBRelation inputRelation = sgbd.getRelation(inputRelName);
		if(inputRelation != null)
			sgbd.createRelation(outputRelName, inputRelation.getRelationSchema().getSort());
	}
}
