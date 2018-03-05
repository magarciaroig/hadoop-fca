package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name="formal_concept")
public class FormalConcept {
	
	@Id
	@Column(name="hash")
	private Integer hash;

	@Column(name="num_objects", nullable=false)
	private Integer numObjects;
	
	@Column(name="num_attbs", nullable=false)
	private Integer numAttributes;
	
	@Column(name="level", nullable=true)
	private Integer level;
	
	@ManyToMany(mappedBy="formalConcepts")
	private Set<Object> objects = new LinkedHashSet<>();
	
	@ManyToMany
	@JoinTable(
	      name="has_attb",
	      joinColumns={@JoinColumn(name="formal_concept_id", referencedColumnName="hash")},
	      inverseJoinColumns={@JoinColumn(name="attb_id", referencedColumnName="id")})
	private Set<Attribute> attbs = new LinkedHashSet<>();

	public Integer getHash() {
		return hash;
	}

	public void setHash(Integer hash) {
		this.hash = hash;
	}

	public Integer getNumObjects() {
		return numObjects;
	}

	public void setNumObjects(Integer numObjects) {
		this.numObjects = numObjects;
	}

	public Integer getNumAttributes() {
		return numAttributes;
	}

	public void setNumAttributes(Integer numAttributes) {
		this.numAttributes = numAttributes;
	}

	public Integer getLevel() {
		return level;
	}

	public void setLevel(Integer level) {
		this.level = level;
	}
	
	public Set<Object> getObjects(){
		return this.objects;
	}
	
	public Set<Attribute> getAttributes(){
		return this.attbs;
	}
}
