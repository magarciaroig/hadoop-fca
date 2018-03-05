package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name="object")
public class Object {
	
	@Id
	@Column(name = "id")
	private Integer id;
	
	@Column(name="hadoop_id", nullable=false, length=36, unique=true)
	private String hadoopId;
	
	@OneToMany(mappedBy = "object")
    private Set<HasField> fields = new HashSet<HasField>();
	
	@ManyToMany
	@JoinTable(
	      name="has_object",
	      joinColumns={@JoinColumn(name="object_id", referencedColumnName="id")},
	      inverseJoinColumns={@JoinColumn(name="formal_concept_id", referencedColumnName="hash")})
	private Set<FormalConcept> formalConcepts = new LinkedHashSet<>();

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getHadoopId() {
		return hadoopId;
	}

	public void setHadoopId(String hadoopId) {
		this.hadoopId = hadoopId;
	}

	public Set<HasField> getFields() {
		return fields;
	}	
	
	public Set<FormalConcept> getFormalConcepts(){
		return formalConcepts;
	}
}
