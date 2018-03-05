package com.mgarciaroig.fca.web.persistence.data.rawmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@Embeddable
public class HasFieldPK implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Column(name = "object_id")
    private Integer object_id;

    @Column(name = "field_id")
    private Long field_id;	
    
    @Override
    public boolean equals(java.lang.Object other){
    	
    	if (other instanceof HasFieldPK){
    		
    		HasFieldPK otherPK = (HasFieldPK) other;
    		
    		EqualsBuilder eb = new EqualsBuilder();
    		
    		eb.append(object_id, otherPK.object_id);
    		eb.append(field_id, otherPK.field_id);
    		
    		return eb.isEquals();
    	}
    	
    	return false;
    }
    
    @Override
    public int hashCode(){
    	return (new HashCodeBuilder()).append(object_id).append(field_id).toHashCode();    
    }
}
