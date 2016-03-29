package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;

public class OrderedProperties extends Properties {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final HashSet<Object> keys = new LinkedHashSet<Object>();

    public OrderedProperties() {
    }

    public Iterable<Object> orderedKeys() {
        return Collections.list(keys());
    }

    public Enumeration<Object> keys() {
        return Collections.<Object>enumeration(keys);
    }

    public Object put(Object key, Object value) {
        keys.add(key);
        return super.put(key, value);
    }
}