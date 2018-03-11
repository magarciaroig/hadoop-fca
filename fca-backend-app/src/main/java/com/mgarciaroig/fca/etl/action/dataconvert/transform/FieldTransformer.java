package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import java.util.HashMap;
import java.util.Map;

import com.mgarciaroig.fca.etl.action.dataconvert.FieldType;

public class FieldTransformer implements Transformer {
		
	private final Transformer commonFieldTypesTransformer = new ToCleanLowercaseTransformer(); 
	private final Transformer longTransformer = new LongFieldTransformer();
	private final Transformer doubleTransformer = new DoubleFieldTransformer();
	private final Transformer booleanTransformer = new BooleanFieldTransformer();
	private final Transformer dateTransformer = new DateFieldTransformer();
	
	private final Map<FieldType,Transformer[]> transformers;
	
	public FieldTransformer(){
		
		transformers = new HashMap<FieldType,Transformer[]>();
		
		initializeTransformers();						
	}			
	
	@Override
	public String transform(final FieldType fieldType, final String toBeTransformed) {
								
		final Transformer[] transformersToApply = transformers.get(fieldType);
		
		String transformed = toBeTransformed;
		
		for (Transformer currentTransformer : transformersToApply){
			
			transformed = currentTransformer.transform(fieldType, transformed);
		}
		
		return transformed;
	}
	
	private void initializeTransformers() {		
		
		final Transformer[] textFieldsTranformers = new Transformer[] {commonFieldTypesTransformer}; 
		
		final Transformer[] longFieldsTranformers = new Transformer[] {commonFieldTypesTransformer, longTransformer};
		
		final Transformer[] doubleFieldsTranformers = new Transformer[] {commonFieldTypesTransformer, doubleTransformer};
		
		final Transformer[] booleanFieldsTranformers = new Transformer[] {commonFieldTypesTransformer, booleanTransformer};
		
		final Transformer[] dateFieldsTranformers = new Transformer[] {commonFieldTypesTransformer, dateTransformer};
		
		transformers.put(FieldType.TEXT_FIELD, textFieldsTranformers);
		transformers.put(FieldType.LONG_FIELD, longFieldsTranformers);
		transformers.put(FieldType.DOUBLE_FIELD, doubleFieldsTranformers);
		transformers.put(FieldType.BOOLEAN_FIELD, booleanFieldsTranformers);
		transformers.put(FieldType.DATE_FIELD, dateFieldsTranformers);
	}

}
