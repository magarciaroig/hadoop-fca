package com.mgarciaroig.fca.util;

import java.util.LinkedHashMap;

/**
 * Haspmap that stores maxSize elements as much
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 * @param <K>
 * @param <V>
 */
public class MaxSizedHashMap <K, V> extends LinkedHashMap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final int maxSize;
    
	public MaxSizedHashMap(int maxSize) {
        this.maxSize = maxSize;
    }
    
    @Override
    public V put(final K key, final V value){
    	
    	removeYoungestElementIfNeeded();
    	
    	super.put(key,  value);
    	
		return value;    	
    }

	private void removeYoungestElementIfNeeded() {
		if (maxNumberOfElementsReached()){
    		removeYoungestElement();    		
    	}
	}

	private void removeYoungestElement() {
		final K candidateToBeRemoved = chooseCandidateToBeRemoved();
		this.remove(candidateToBeRemoved);
	}

	private K chooseCandidateToBeRemoved() {
		return this.keySet().iterator().next();
	}

	private boolean maxNumberOfElementsReached() {
		return this.size() >= getMaxSize();
	}
    
    public int getMaxSize() {
		return maxSize;
	}        
}
