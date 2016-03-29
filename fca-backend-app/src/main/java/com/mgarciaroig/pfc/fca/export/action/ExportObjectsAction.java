package com.mgarciaroig.pfc.fca.export.action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Action to export all object and fields data from hadoop cluster to database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportObjectsAction extends DatabaseExportActionBase {
			
	private static final String etlHdfsConvertedFileProperty = "etlHdfsConvertedFile";
	private static final String etlHdfsDiscretizedDirProperty = "etlHdfsDiscretizedDir";
	
	private Map<String,Integer> objectFieldsPersistenceInfo = null;
	
	public static void main(String[] args) throws IOException, FCAAnalizerException {
		
		ExportObjectsAction action = new ExportObjectsAction(args);
		
		action.run();
	}

	protected ExportObjectsAction(String[] args) throws IOException {
		super(args);
	}

	@Override
	protected void run() throws FCAAnalizerException {
																																
		try (Connection con = connectToDatabase()){
				
			cleanUpDatabase(con);
				
			persistObjects(con, readObjectIds());
				
			con.commit();				
		
		} catch (IllegalArgumentException | IOException | InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException  rawError) {
			
			throw new ExportObjectsException(rawError);
		}		
	}
	
	private void persistObjects(final Connection con, final List<String> readObjectIds) throws IOException, SQLException {
		
		final Iterator<String> objectIdsIterator = readObjectIds.iterator();
						
		final Configuration conf = super.getConfiguration();
		final Option filePath = SequenceFile.Reader.file(new Path(super.getPropertyFromConfiguration(etlHdfsConvertedFileProperty)));	
				
		
		try (final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath)){
			
			final Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
			final SortedMapWritable value = new SortedMapWritable();
			
			while (sequenceFileReader.next(key, value)){
												
				final SortedMapWritable record = (SortedMapWritable) value;
					
				persistObject(con, objectIdsIterator.next(), record);				
			}						
		}
		
	}
	
	private void persistObject(final Connection con, final String hdfsObjectId, final SortedMapWritable record) throws SQLException{
				
		final Integer objectId = insertObject(con, hdfsObjectId);
		
		if (this.objectFieldsPersistenceInfo == null){
			this.objectFieldsPersistenceInfo = loadFieldsPersistenceInfo(con, record);
		}
		
		persistObjectFields(con, objectId, record);				
	}

	private void persistObjectFields(final Connection con, final Integer objectId, final SortedMapWritable record) throws SQLException {
				
		
		for (@SuppressWarnings("rawtypes") final Entry<WritableComparable, Writable> currentAttbData : record.entrySet()){
			
			final String fieldName = currentAttbData.getKey().toString().trim();
			
			final Integer fieldId = this.objectFieldsPersistenceInfo.get(fieldName);
			final String fieldValue = persistableString(currentAttbData.getValue().toString());
			
			try (final PreparedStatement hasAttbSt = con.prepareStatement("insert into has_field(object_id,field_id,value) values (?,?,?)")){
				
				hasAttbSt.setInt(1, objectId);
				hasAttbSt.setInt(2, fieldId);				
				hasAttbSt.setString(3, fieldValue);
				
				hasAttbSt.executeUpdate();
			}			
		}
	}

	private String persistableString(final String toBePersisted) {
		
		String persistable = toBePersisted.trim();
		
		if (persistable.equals(NullWritable.get().toString())){
			persistable = null;
		}
		
		return persistable;
	}
	
	private Map<String, Integer> loadFieldsPersistenceInfo(final Connection con, final SortedMapWritable record) throws SQLException {
		
		final Map<String, Integer> fieldsPersistenceInfo = new TreeMap<>();
		
		for (@SuppressWarnings("rawtypes") final WritableComparable currentAttb : record.keySet()){
			
			final String attbName = currentAttb.toString();
			final Integer fieldId = insertField(con, attbName);
			
			fieldsPersistenceInfo.put(attbName, fieldId);
		}
				
		return fieldsPersistenceInfo;
	}

	private Integer insertField(final Connection con, final String attbName) throws SQLException {
		
		try (final PreparedStatement insertFieldSt = con.prepareStatement("insert into field (name) values (?)", 1)){
			
			insertFieldSt.setString(1, attbName.trim());
			
			insertFieldSt.executeUpdate();
			
			return lastInsertId(insertFieldSt);						
		}	
	}

	private int insertObject(final Connection con, final String hdfsObjectId) throws SQLException{
		
		try (final PreparedStatement insertObjectSt = con.prepareStatement("insert into object (hadoop_id) values (?)", 1)){
			
			insertObjectSt.setString(1, hdfsObjectId);
			
			insertObjectSt.executeUpdate();
			
			return lastInsertId(insertObjectSt);						
		}	
		
	}	
	
	private void cleanUpDatabase(final Connection con) throws SQLException{
		
		final String[] toCleanUpTables = {"object", "field"};
		
		super.cleanUpDatabase(con, toCleanUpTables);		
	}	
	
	private List<String> readObjectIds() throws FileNotFoundException, IllegalArgumentException, IOException{
		
		final List<String> objectIds = new ArrayList<>();
		
		final FileSystem fs = super.getFileSystem();
		
		final String discretizedObjectsPathName = super.getPropertyFromConfiguration(etlHdfsDiscretizedDirProperty);
		
		final FileStatus[] discretizedObjectFiles = fs.listStatus(new Path(discretizedObjectsPathName), new PathFilter(){

			@Override
			public boolean accept(final Path newPath) {
				
				return newPath.getName().startsWith("part");
			}
			
		});
		
		for (final FileStatus currentDiscretizedObjectsFile : discretizedObjectFiles){
			populateObjectIds(objectIds, currentDiscretizedObjectsFile.getPath());
		}								
		
		return objectIds;
	}
	
	private void populateObjectIds(final List<String> objectIds, final Path currentDiscretizedObjectsPath) throws IOException {					
		
		final Text idFieldName = new Text("id");
		
		final Configuration conf = super.getConfiguration();
		final Option filePath = SequenceFile.Reader.file(currentDiscretizedObjectsPath);		
		
		try (final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath)){

			final Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
			final Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);
			
			while (sequenceFileReader.next(key, value)){
				
				if (value instanceof SortedMapWritable){
					
					final String currentObjectId = ((SortedMapWritable) value).get(idFieldName).toString();
					objectIds.add(currentObjectId);
				}
			}
		}		
	}

}
