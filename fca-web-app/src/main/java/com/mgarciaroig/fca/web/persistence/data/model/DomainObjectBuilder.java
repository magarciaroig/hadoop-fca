package com.mgarciaroig.fca.web.persistence.data.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.mgarciaroig.fca.web.persistence.data.rawmodel.FormalConcept;
import com.mgarciaroig.fca.web.persistence.data.rawmodel.HasField;
import com.mgarciaroig.fca.web.persistence.data.rawmodel.Object;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.mgarciaroig.fca.web.persistence.data.repo.ObjectRepository;

@Component
public class DomainObjectBuilder {
		
	@Autowired
	private ObjectRepository objectRepo;
	
	@Autowired
	private HibernateTransactionManager transactionmanager;
	
	@Transactional
	public DomainObject build(final String hadoopObjectid) throws IOException, SQLException{
		
		final Object rawObject = objectRepo.findByHadoopId(hadoopObjectid);
		
		final DomainObject domainObject = new DomainObject(rawObject.getHadoopId());
		
		fillFields(rawObject, domainObject);
		
		fillSimilitudes(rawObject, domainObject);
		
		return domainObject;
	}

	private void fillFields(final Object rawObject, final DomainObject domainObject) {
		
		for (final HasField currentFieldData : rawObject.getFields()){
			
			domainObject.addField(currentFieldData.getField().getName(), currentFieldData.getValue());
		}
	}
	
	private void fillSimilitudes(final Object rawObject, final DomainObject domainObject) throws SQLException {
						
		int maxNumberOfAttbs = objectRepo.findNumberOfAttbsOfMoreRelevantConcept(rawObject.getHadoopId());
		final List<FormalConcept> concepts = objectRepo.findConceptsByNumberOfAttbs(rawObject.getHadoopId(), maxNumberOfAttbs);
						
		final Connection con = transactionmanager.getDataSource().getConnection();
		
		final Set<Integer> previousConcepts = new HashSet<>();
									
		fillSimilitudesInRelevanceOrder(con, conceptIds(concepts), previousConcepts, domainObject);
	}
	
	private void fillSimilitudesInRelevanceOrder(final Connection con, 
			final Set<Integer> conceptIds, 
			final Set<Integer> previousConcepts, 
			final DomainObject domainObject) throws SQLException 
	{				
		conceptIds.removeAll(previousConcepts);
		
		if (conceptIds.isEmpty()) return;
				
		final Map<Integer,Collection<String>> objectData = allObjectData(con, conceptIds);				
		final Map<Integer,Collection<String>> attbData = allAttbData(con, conceptIds);
		
		for (final Entry<Integer, Collection<String>> currentObjectEntry : objectData.entrySet()){
			
			final int formalConceptId = currentObjectEntry.getKey();
									
			final Collection<String> objectIds = currentObjectEntry.getValue();			
			final Collection<String> attbs = attbData.get(formalConceptId);
			
			if (validSimilarityData(objectIds, attbs)){
								
				domainObject.addSimilitude(objectIds, attbs);				
			}
			
			previousConcepts.add(formalConceptId);			
		}
				
		final Set<Integer> parentIds = parents(con, conceptIds); 
				
		fillSimilitudesInRelevanceOrder(con, parentIds, previousConcepts, domainObject);
	}

	private boolean validSimilarityData(final Collection<String> objectIds, final Collection<String> attbs) {
		return objectIds != null && !objectIds.isEmpty() && attbs != null && !attbs.isEmpty();
	}
	
	private Set<Integer> parents(final Connection con, final Set<Integer> conceptIds) throws SQLException {
		
		final Set<Integer> parentIds = new TreeSet<>();
				
		final StringBuilder sql = new StringBuilder();
		sql.append("select parent_id ");
		sql.append("from formal_concept_hierarchy ");
		sql.append("where formal_concept_id in (");
		sql.append(StringUtils.join(conceptIds, ","));
		sql.append(")");	
		
		try (final Statement st = con.createStatement()){
			
			try (final ResultSet rs = st.executeQuery(sql.toString())){
				
				while (rs.next()){
					parentIds.add(rs.getInt(1));										
				}
			}			
		}
		
		return parentIds;
	}

	//Note: In native sql to gain performance (all object for all concepts in a single query)
	private Map<Integer,Collection<String>> allObjectData(Connection con, final Set<Integer> conceptIds) throws SQLException {
		
		final Map<Integer,Collection<String>> objectIdsStore = new LinkedHashMap<>();		
		
		StringBuilder sql = new StringBuilder();
		
		sql.append("select o.hadoop_id, ho.formal_concept_id from has_object ho ");
		sql.append("inner join object o on ho.object_id = o.id ");
		sql.append("where ho.formal_concept_id in (");
		sql.append(StringUtils.join(conceptIds, ","));
		sql.append(')');
					
		try (final Statement st = con.createStatement()){
									
			try (final ResultSet rs = st.executeQuery(sql.toString())){
				
				while (rs.next()){
					final String hadoopObjectId = rs.getString(1);
					final int formalConceptId = rs.getInt(2);
					
					Collection<String> objects = objectIdsStore.get(formalConceptId);
					
					if (objects == null){
						objects = new ArrayList<String>();
						objectIdsStore.put(formalConceptId, objects);
					}
					
					objects.add(hadoopObjectId);										
				}
			}			
		}
		
		return objectIdsStore;
	}
	
	//Note: In native sql to gain performance (all attb for all concepts in a single query)
	private Map<Integer,Collection<String>> allAttbData(Connection con, final Set<Integer> conceptIds) throws SQLException {
		
		final Map<Integer,Collection<String>> attbsNamesStore = new LinkedHashMap<>();		
		
		StringBuilder sql = new StringBuilder();
		
		sql.append("select a.name, ha.formal_concept_id from has_attb ha ");
		sql.append("inner join attribute a on ha.attb_id = a.id ");
		sql.append("where ha.formal_concept_id in (");
		sql.append(StringUtils.join(conceptIds, ","));
		sql.append(')');
					
		try (final Statement st = con.createStatement()){
									
			try (final ResultSet rs = st.executeQuery(sql.toString())){
				
				while (rs.next()){
					final String attbName = rs.getString(1);
					final int formalConceptId = rs.getInt(2);
					
					Collection<String> attributes = attbsNamesStore.get(formalConceptId);
					
					if (attributes == null){
						attributes = new ArrayList<String>();
						attbsNamesStore.put(formalConceptId, attributes);
					}
					
					attributes.add(attbName);										
				}
			}			
		}
		
		return attbsNamesStore;
	}
	
	private Set<Integer> conceptIds (final Collection<FormalConcept> concepts){
				
		final Set<Integer> ids = new TreeSet<>();
				
		for (final FormalConcept currentConcept : concepts){
			ids.add(currentConcept.getHash());
		}
		
		return ids;		
	}			
}
