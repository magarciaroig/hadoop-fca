package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapsId;
import javax.persistence.Table;

import java.lang.*;

@Entity
@Table(name = "has_field")
public class HasField {

	@EmbeddedId
	private HasFieldPK id;

	@ManyToOne
	@MapsId("object_id") // This is the name of attr inHasFieldPK class
	@JoinColumn(name = "object_id")
	private Object object;

	@ManyToOne
	@MapsId("field_id")
	@JoinColumn(name = "field_id")
	private Field field;
	
	@Column(name="value", length=200, nullable=true)
	private String value;

	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public Field getField() {
		return field;
	}

	public void setField(Field field) {
		this.field = field;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}			
}
