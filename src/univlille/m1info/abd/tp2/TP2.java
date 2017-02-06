package univlille.m1info.abd.tp2;

import java.util.HashMap;
import java.util.Map;

import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

public class TP2 {

	private boolean contains(String str, String[] array){
		for(String string : array)
			if(str.equals(string))
				return true;
		return false;
	}
	
	/** Fait une copie des données d'une relation vers une nouvelle relation.
	 * Retourne le nom de la nouvelle relation.
	 * @param sgbd
	 * @param relName
	 * @return
	 */
	public String copyRelation (SimpleSGBD sgbd, String relName) {
		// Mettre la relation en entrée et son schéma dans des variables pour pouvoir les utiliser plus facilement
		SimpleDBRelation rel = sgbd.getRelation(relName);
		RelationSchema relSchema = rel.getRelationSchema();
		
		// Générer un nom pour la nouvelle relation (évite de devoir en créer un)
		String newRelName = sgbd.getFreshRelationName();
	
		// Créer la nouvelle relation avec la même sorte que celle de la relation copiée
		SimpleDBRelation newRel = sgbd.createRelation(newRelName, sgbd.getRelation(relName).getRelationSchema().getSort());
	
		// Mettre le schéma de la nouvelle relation dans une variable pour pouvoir l'utiliser plus facilement
		RelationSchema newRelSchema = newRel.getRelationSchema();
		
		// Copier tous les tuples de la relation en entrée vers la nouvelle relation
		rel.switchToReadMode();
		newRel.switchToWriteMode();   // Normalement pas nécessaire, puisqu'à la création elle est déjà en mode écriture
		
		String[] relTuple;
		while ((relTuple = rel.nextTuple()) != null) {  // Tant qu'il reste des tuples dans rel
			// Créer un tuple vide pour la nouvelle relation
			String[] newRelTuple = newRelSchema.newEmptyTuple();
			// Copier les valeurs des attributs du tuple en entrée vers le nouveau tuple
			for (String attrName : relSchema.getSort()) {
				String attrValue = relSchema.getAttributeValue(relTuple, attrName);
				newRelSchema.setAttributeValue(attrValue, newRelTuple, attrName);
			}
			// Noter que ci-dessus il faut copier les valeurs des attributs
			// On ne peut pas simplement prendre relTuple et l'ajouter dans newRelTuple, car
			// dans ce cas les deux tuples seront des références vers le même tableau en mémoire
			// et une modification de l'un va aussi modifier l'autre
			
			// Ajouter le nouveau tuple à la nouvelle relation
			newRel.addTuple(newRelTuple);
		}
		
		// La copie est finie, retourner le nom de la nouvelle relation
		return newRelName;
		
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
		projection.switchToWriteMode();
		
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
		selectedRelation.switchToWriteMode();
		
		for(String[] tuple = relation.nextTuple(); tuple != null; tuple = relation.nextTuple())
			if(tuple[attribute].equals(value))
				selectedRelation.addTuple(tuple);
		
		sgbd.addRelation(selectedRelationName, selectedRelation);
		return selectedRelationName;
	}
	
	public String computeJoin(SimpleSGBD sgbd, String inputRelName1, String inputRelName2){
		int index = -1;
		SimpleDBRelation relation1 = sgbd.getRelation(inputRelName1), relation2 = sgbd.getRelation(inputRelName2);
		String[] sorts1 = relation1.getRelationSchema().getSort(), sorts2 = relation2.getRelationSchema().getSort();
		
		/*
		 * Récupère l'indice de l'attribut commun sur lequel on fera la jointure.
		 */
		for(int i = 0; i < sorts1.length; i++)
			for(int j = 0; j < sorts2.length; j++)
				if(sorts1[i].equals(sorts2[j]))
					index = j;
		
		if(index < 0)
			return null;
		
		/*
		 * Associe à la relation de sortie les attributs des relations en entrée.
		 */
		int i = 0, j = 0;
		String[] outputSorts = new String[sorts1.length + sorts2.length - 1];
		for(i = 0; i < sorts1.length; i++){
			outputSorts[i] = sorts1[i];
			System.out.print(sorts1[i] + " ");
		}
			
		for(String str : sorts2){
			if(j != index)
				outputSorts[i] = str;
			i++; j++;
		}

		String outputRelationName = sgbd.getFreshRelationName();
		SimpleDBRelation outputRelation = new SimpleDBRelation(new DefaultRelationSchema(outputRelationName, outputSorts));
		
		relation1.switchToReadMode();
		relation2.switchToReadMode();
		outputRelation.switchToWriteMode();
		
		/*
		 * Copie des tuples dans la relation de sortie.
		 */
		for(String[] tuple1 = relation1.nextTuple(); tuple1 != null; tuple1 = relation1.nextTuple()){
			for(String[] tuple2 = relation2.nextTuple(); tuple2 != null; tuple2 = relation2.nextTuple()){
				 String[] newTuple = new String[tuple1.length + tuple2.length - 1];
				 for(i = 0; i < tuple1.length; i++)
					 newTuple[i] = tuple1[i];
				 for(j = 0; j < tuple2.length; j++, i++)
					 if(j != index)
						 newTuple[i] = tuple2[j];
				 outputRelation.addTuple(newTuple);
			}
		}
		
		outputRelation.switchToReadMode();
		
		sgbd.addRelation(outputRelationName, outputRelation);
		
		return outputRelationName;
	}
	
	public void copyRelation(SimpleSGBD sgbd, String inputRelName, String outputRelName){
		SimpleDBRelation inputRelation = sgbd.getRelation(inputRelName);
		if(inputRelation != null)
			sgbd.createRelation(outputRelName, inputRelation.getRelationSchema().getSort());
	}
}
