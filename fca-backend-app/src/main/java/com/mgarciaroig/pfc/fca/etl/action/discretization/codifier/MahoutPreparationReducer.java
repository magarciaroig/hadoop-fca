package com.mgarciaroig.pfc.fca.etl.action.discretization.codifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.mahout.math.ConstantVector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Reducer to collect all the mahout clusteriable values for a given field, and generate a valid mahout input file
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class MahoutPreparationReducer extends Reducer<Text,DoubleWritable,NullWritable,VectorWritable>{

	private MultipleOutputs<NullWritable,VectorWritable> mos;
	private VectorWritable dataHolder = new VectorWritable();
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable,VectorWritable>(context);
    }
	
	/**
	 * Populates mahout VectorWritable, and persists it to create a valid mahout input file 
	 */
	@Override
	public void reduce(final Text fieldName, final Iterable<DoubleWritable> fieldValues, final Context context) throws IOException, InterruptedException {
		
		final List<Double> uniqueValues = new ArrayList<Double>();
		final HashMap<Double,Double> collectedValuesMap = new HashMap<Double,Double>(); 
	
		for (DoubleWritable currentValue : fieldValues){
			
			final Double rawValue = currentValue.get();			
			final boolean valuePendingToBeCollected = !collectedValuesMap.containsKey(rawValue);
			
			if (valuePendingToBeCollected){
			
				collectedValuesMap.put(rawValue, rawValue);
				uniqueValues.add(rawValue);
			}						
			
			dataHolder.set(populateDataToEmit(currentValue));
									
			mos.write(NullWritable.get(), dataHolder, buildNamedOutput(fieldName.toString()));
		}	
		
		persistCanopyThresoldValues(uniqueValues, fieldName.toString());
	}

	private void persistCanopyThresoldValues(final List<Double> uniqueValues, final String fieldName) throws IOException, InterruptedException {
		
		Collections.sort(uniqueValues);
		
		final Double[] orderedValues = uniqueValues.toArray(new Double[]{});
		
		final Double t1 = Q3quartile(orderedValues);
		final Double t2 = t1/2.0d;
		
		final Vector quartileData = new DenseVector(new double[]{t1,t2});
		
		dataHolder.set(quartileData);
		
		mos.write(NullWritable.get(), dataHolder, buildCanopyThresoldNamedOutput(fieldName));
	}
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {		
		mos.close();
    }
	
	private Vector populateDataToEmit(final DoubleWritable currentValue) {
		
		final int numberOfElements = 1;
		return new ConstantVector(currentValue.get(), numberOfElements);						
	}
	
	private Double Q3quartile(Double[] orderedValues) {
		
		final int q3PctQuartile = 75;
		return quartile(orderedValues, q3PctQuartile);
	}
		
	
	private Double quartile(Double[] orderedValues, int lowerPercent) {
                
        int quartilePosition = (int) Math.round(orderedValues.length * lowerPercent / 100);
        
        return orderedValues[quartilePosition];
    }			
	
	private String buildNamedOutput(final String stringfiedFieldName){
		return "./".concat(stringfiedFieldName);
	}			
	
	private String buildCanopyThresoldNamedOutput(final String stringfiedFieldName){
		return buildNamedOutput(stringfiedFieldName).concat(".canopy_thresold");		
	}
}
