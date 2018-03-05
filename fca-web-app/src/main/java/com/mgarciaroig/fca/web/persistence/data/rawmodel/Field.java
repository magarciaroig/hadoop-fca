package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name="field")
public class Field {
	
	@Id
	@Column(name = "id")
	private Integer id;
	
	@Column(name="name", length=100,nullable=false)
	private String name;
	
	@OneToMany(mappedBy = "field")
    private Set<HasField> objects = new HashSet<HasField>(); 
		
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<HasField> getObjects() {
		return objects;
	}			
}
