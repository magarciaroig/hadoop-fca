package com.mgarciaroig.pfc.fca.web.persistence.data.repo;

import java.util.List;

import com.mgarciaroig.pfc.fca.web.persistence.data.rawmodel.Object;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.RepositoryDefinition;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.mgarciaroig.pfc.fca.web.persistence.data.rawmodel.FormalConcept;

@Transactional
@RepositoryDefinition(domainClass=Object.class, idClass=Integer.class)
public interface ObjectRepository extends CrudRepository<Object, Integer> {

	Object findByHadoopId(final String hadoopObjectId);
	
	@Query("select max(c.numAttributes) FROM Object o JOIN o.formalConcepts c where o.hadoopId = :hadoopObjectId and c.numObjects > 1 and level is not null")
	Integer findNumberOfAttbsOfMoreRelevantConcept(@Param("hadoopObjectId") final String hadoopObjectId);
		
	@Query("select c FROM Object o JOIN o.formalConcepts c where o.hadoopId = :hadoopObjectId and c.numAttributes = :numberOfAttbs ")
	List<FormalConcept> findConceptsByNumberOfAttbs(
			@Param("hadoopObjectId") final String hadoopObjectId, 
			@Param("numberOfAttbs") final int numberOfAttbs);
}
