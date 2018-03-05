package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="attribute")
public class Attribute {

	@Id
	@Column(name = "id")
	private Integer id;
	
	@Column(name="name", length=100,nullable=false)
	private String name;
			
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
}
