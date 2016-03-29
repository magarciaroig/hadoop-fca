package com.mgarciaroig.pfc.fca.analysis.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.io.Writable;

import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

/**
 * Class to modelize a FCA formal concept
 *  
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalConcept extends IntentManager implements Writable  {
	
	private TreeSet<String> objectIds;
	private TreeSet<String> sharedAttbs;
	private Integer cachedHashCode;
	
	/**
	 * Default constructor (needed by serialization utilities)
	 */
	public FormalConcept(){
		
		reset();
	}
	
	/**
	 * Constructor used for explicitly set the intent and extent for the new formal concept. It us useful in order to create the maximun
	 * and minimun formal concepts in a cheap way
	 * @param objectIds
	 * @param sharedAttbs
	 */
	public FormalConcept(final List<String> objectIds, List<String> sharedAttbs){
		this.cachedHashCode = null;
		this.objectIds = new TreeSet<>(objectIds);
		this.sharedAttbs  = new TreeSet<>(sharedAttbs);
	}
				
	/**
	 * Build formal context from object list (calculates shared attributes from objects)
	 * @param objects
	 * @throws FormalConceptNonConsistentAttributesException 
	 */
	public FormalConcept(final Collection<FormalContextItem> objects) throws FormalConceptNonConsistentAttributesException{
		this(objects, null);
	}
	
	/**
	 * Build formal context by object list and shared attributes list (tipically used in the FCA first step)
	 * @param objects
	 * @param sharedAttbs
	 * @throws FormalConceptNonConsistentAttributesException 
	 */
	public FormalConcept(final Collection<FormalContextItem> objects, final Collection<String> sharedAttbs) throws FormalConceptNonConsistentAttributesException{
		
		this.cachedHashCode = null;
		this.objectIds = populateObjectIds(objects);
		
		if (sharedAttbs != null){
			
			checkObjectsAndAttributesConsistencyGuard(objects, sharedAttbs);		
			
			this.sharedAttbs = populateAttbs(sharedAttbs);
		}
		else {
			this.sharedAttbs = populateAttbsFromObjects(objects);
		}
	}			

	/**
	 * Build and new formal context adding a new attribute
	 * 
	 * @param objectRepository
	 * @param newAttb
	 * @return
	 * @throws IOException
	 * @throws FormalConceptNonConsistentAttributesException 
	 */
	public FormalConcept deriveNewFormalConcept(final FormalContextItemRepository objectRepository, final String newAttb) throws IOException, FormalConceptNonConsistentAttributesException{
		
		List<FormalContextItem> objects = new ArrayList<FormalContextItem>();
								
		final List<String> allObjectIdsWithNewAttb = objectRepository.findObjectIdsByAttribute(newAttb);
				
		final List<String> myObjectIdsWithNewAttb = myObjectIdsIntersectionWith(allObjectIdsWithNewAttb);
				
		if (myObjectIdsWithNewAttb.size() > 0){
			objects = objectRepository.find(myObjectIdsWithNewAttb);
		}
					
		return new FormalConcept(objects);
	}
	
	/**
	 * Build a new key adding a new attribute
	 * @param newAttribute
	 * @return
	 */
	public FormalConceptBuildingKey deriveNewBuildingKey(final String newAttribute){
		return new FormalConceptBuildingKey(this.sharedAttbs, newAttribute);
	}
	
	public TreeSet<String> getObjects(){
		return new TreeSet<String>(this.objectIds);
	}
	
	public TreeSet<String> getAttributes(){
		return new TreeSet<String>(this.sharedAttbs);
	}
	
	public boolean hasObject(final String objectId){
		return containsElement(objectIds, objectId);
	}
	
	public boolean hasAttb(final String attb){
		return containsElement(sharedAttbs, attb);
	}	
	
	public Set<String> attributesUpTo(final String toAttribute){
		return super.attributesUpTo(this.sharedAttbs, toAttribute);
	}
	
	public boolean extentIsEmpty(){
		return this.objectIds.isEmpty();
	}
	
	@Override
	public String toString(){
		
		String separator = ", ";
		
		final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
		
		sb.append(" [");
		
		sb.append("objects: {");		
		sb.append(StringUtils.join(this.objectIds, separator));
		
		sb.append("}, atributes: {");
		sb.append(StringUtils.join(this.sharedAttbs, separator));
		
		sb.append("} ]");
		
		return sb.toString();
	}			
	
	@Override
	public boolean equals(final Object other){
		
		boolean weAreEquivalent = false;
		
		if (other instanceof FormalConcept){
			
			final FormalConcept otherFormalConcept = (FormalConcept) other;
			
			final EqualsBuilder equalsBuilder = new EqualsBuilder();
			
			equalsBuilder.append(this.objectIds, otherFormalConcept.objectIds);
			equalsBuilder.append(this.sharedAttbs, otherFormalConcept.sharedAttbs);
			
			weAreEquivalent = equalsBuilder.isEquals();
		}
		
		return weAreEquivalent;
	}
	
	@Override
	public int hashCode(){
		
		if (this.cachedHashCode == null){
			this.cachedHashCode = toString().hashCode();
		}
		
		return this.cachedHashCode;
	}
	
	@Override
	public void readFields(final DataInput in) throws IOException {
		
		reset();
		
		int itemsToRead = in.readInt();
		
		for (int currentObjectIndex = 0; currentObjectIndex < itemsToRead; currentObjectIndex++){
			this.objectIds.add(in.readUTF());
		}
		
		itemsToRead = in.readInt(); 				
		
		for (int currentAttbIndex = 0; currentAttbIndex < itemsToRead; currentAttbIndex++){
			this.sharedAttbs.add(in.readUTF());
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		
		out.writeInt(this.objectIds.size());
		
		for (final String currentObjectId : this.objectIds){
			out.writeUTF(currentObjectId);
		}
		
		out.writeInt(this.sharedAttbs.size());
		
		for (final String currentAttb : this.sharedAttbs){
			out.writeUTF(currentAttb);
		}		
	}
	
	private void reset(){
		this.cachedHashCode = null;
		this.objectIds = new TreeSet<String>();
		this.sharedAttbs = new TreeSet<String>();
	}			
	
	private void checkObjectsAndAttributesConsistencyGuard(final Collection<FormalContextItem> objects, final Collection<String> sharedAttbs) 
			throws FormalConceptNonConsistentAttributesException
	{
		for (final String currentAttb : sharedAttbs){
			
			checkAtributeSharedByAllGuard(objects, currentAttb);			
		}		
	}
	
	private void checkAtributeSharedByAllGuard(final Collection<FormalContextItem> objects, final String sharedAttb) throws FormalConceptNonConsistentAttributesException{
		
		for (final FormalContextItem currentObject : objects){
			
			if (!currentObject.hasAttb(sharedAttb)) {
				throw new FormalConceptNonConsistentAttributesException(objects, sharedAttbs);
			}
		}
	}
	
	private boolean containsElement(final Collection<String> store, final String element){
		return store.contains(element);
	}
	
	private TreeSet<String> populateAttbs(final Collection<String> attbs) {
				
		final TreeSet<String> nonRepeatedAttbs = new TreeSet<String>();
		
		for (final String currentAttb : attbs){
			nonRepeatedAttbs.add(currentAttb);			
		}	
		
		return nonRepeatedAttbs;
	}
	
	private TreeSet<String> populateAttbsFromObjects(final Collection<FormalContextItem> objects) {
		
		final TreeSet<String> attbsSharedByAll = new TreeSet<String>();
		
		if (noObjectsAvailable(objects)) return attbsSharedByAll;
								
		for (final String currentAttb : allAttbNamesInOrder(objects)){
									
			if (attbIsSharedByAll(objects, currentAttb)){
				attbsSharedByAll.add(currentAttb);
			}
		}		
		
		return attbsSharedByAll;
	}
	
	private List<String> myObjectIdsIntersectionWith(final List<String> otherObjectIds){
		
		final ArrayList<String> intersected = new ArrayList<String>(otherObjectIds);
		
		intersected.retainAll(this.objectIds);
		
		return intersected;
	}

	private boolean attbIsSharedByAll(final Collection<FormalContextItem> objects, final String currentAttb) {
		
		final Iterator<FormalContextItem> it = objects.iterator();
		
		boolean attSharedByAll = true;
		
		while (it.hasNext()){
			if (!it.next().hasAttb(currentAttb)){
				attSharedByAll = false;
				break;
			}
		}
		
		return attSharedByAll;
	}
	
	private List<String> allAttbNamesInOrder(final Collection<FormalContextItem> objects){
		
		if (noObjectsAvailable(objects)) return new ArrayList<String>();
		
		return objects.iterator().next().allAttbNamesInOrder();
	}	

	private boolean noObjectsAvailable(final Collection<FormalContextItem> objects) {
		
		return objects == null || objects.size() < 1;
	}
	
	private TreeSet<String> populateObjectIds(final Collection<FormalContextItem> objects){
			
		final TreeSet<String> ids = new TreeSet<String>();
		
		for (final FormalContextItem currentObject : objects){
									
			ids.add(currentObject.getObjectId());						
		}
		
		return ids;
	}	
}
