package com.mgarciaroig.fca.analysis.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class FormalConceptBuildingKey extends IntentManager implements WritableComparable {
	
	private TreeSet<String> intent;
	
	private String buildingAttribute;
	
	public FormalConceptBuildingKey(){
		reset();
	}
	
	public FormalConceptBuildingKey (final TreeSet<String> intent, final String buildingAttribute){
		
		this.intent = intent;
		this.buildingAttribute = buildingAttribute;
	}		
	
	public String getBuildingAttribute(){
		return this.buildingAttribute;
	}
	
	public boolean attributeIncludedInIntent(final String attribute){
		return intent.contains(attribute);
	}		
	
	public boolean canonicityTest(final FormalConcept concept){
		
		final Set<String> attributesFromMe = attributesUpTo();
		final Set<String> attributesFromConcept = concept.attributesUpTo(getBuildingAttribute());
		
		return attributesFromMe.equals(attributesFromConcept);		
	}
	
	public FormalConceptBuildingKey deriveNewBuildingKey(final String newAttribute){
		return new FormalConceptBuildingKey(this.intent, newAttribute);
	}
	
	private Set<String> attributesUpTo(){		
		return super.attributesUpTo(this.intent, this.getBuildingAttribute());
	}
			
	@Override
	public String toString(){
		
		final StringBuilder builder = new StringBuilder(getClass().getSimpleName());
		
		builder.append(" [");
		
		builder.append("intent: ");
		builder.append(StringUtils.join(this.intent, ","));
		
		builder.append(" buildingAttribute: ");
		builder.append(buildingAttribute);
		
		builder.append(" ]");
		
		return builder.toString();
	}
	
	@Override
	public boolean equals(final Object other){
		
		boolean weAreEquivalent = false;
		
		if (other instanceof FormalConceptBuildingKey){
			
			final FormalConceptBuildingKey otherKey = (FormalConceptBuildingKey) other;
			
			final EqualsBuilder builder = new EqualsBuilder();
			
			builder.append(this.intent, otherKey.intent);
			builder.append(this.buildingAttribute, otherKey.buildingAttribute);
			
			weAreEquivalent = builder.isEquals();
		}
		
		return weAreEquivalent;
	}
	
	@Override
	public int hashCode(){
		return toString().hashCode();
	}

	@Override
	public void readFields(final DataInput in) throws IOException {
		
		reset();
		
		final int intentSize = in.readInt();
		
		for (int currentIntentIndex = 0; currentIntentIndex < intentSize; currentIntentIndex++){
			this.intent.add(in.readUTF());
		}
		
		this.buildingAttribute = in.readUTF();		
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		
		out.writeInt(this.intent.size());
		
		for (final String currentAttribute : this.intent){
			out.writeUTF(currentAttribute);
		}
						
		out.writeUTF(this.buildingAttribute);		
	}

	@Override
	public int compareTo(final Object other) {
		
		if (! (other instanceof FormalConceptBuildingKey) ) return -1;
		
		final FormalConceptBuildingKey otherKey = (FormalConceptBuildingKey) other;
		
		final CompareToBuilder builder = new CompareToBuilder();
		
		String attributeSeparator = ",";
		
		builder.append(StringUtils.join(this.intent,attributeSeparator), StringUtils.join(otherKey.intent,attributeSeparator));
		
		if (builder.toComparison() == 0){
			builder.append(this.buildingAttribute, otherKey.buildingAttribute);
		}
				
		return builder.toComparison();
	}	
	
	private void reset(){
		intent = new TreeSet<>();
		buildingAttribute = "";
	}		
}
