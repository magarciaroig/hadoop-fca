package com.mgarciaroig.pfc.fca.analysis.action.export;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;
import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

/**
 * Action to export all object and attributes data from hadoop cluster to database
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportObjectsAction extends BaseAction {
	
	private static final String databaseDriverProperty = "jdbc.driver.name";
	private static final String databaseConnectionStringProperty = "jdbc.connection.string";	
	
	private static final String etlHdfsConvertedFileProperty = "etlHdfsConvertedFile";
	private static final String etlHdfsDiscretizedDirProperty = "etlHdfsDiscretizedDir";
	
	private Map<String,Integer> objectAttbsPersistenceInfo = null;
	
	public static void main(String[] args) throws IOException, FCAAnalizerException{
		
		ExportObjectsAction action = new ExportObjectsAction(args);
		
		action.run();
	}

	protected ExportObjectsAction(String[] args) throws IOException {
		super(args);
	}

	@Override
	protected void run() throws FCAAnalizerException {
						
		try {			
																							
			try (Connection con = connectToDatabase()){
				
				cleanUpDatabase(con);
				
				persistObjects(con, readObjectIds());
				
				con.commit();				
			} 
									
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
		
		if (this.objectAttbsPersistenceInfo == null){
			this.objectAttbsPersistenceInfo = loadAttbsPersistenceInfo(con, record);
		}
		
		persistObjectAttbs(con, objectId, record);				
	}

	private void persistObjectAttbs(final Connection con, final Integer objectId, final SortedMapWritable record) throws SQLException {
				
		
		for (@SuppressWarnings("rawtypes") final Entry<WritableComparable, Writable> currentAttbData : record.entrySet()){
			
			final String attbName = currentAttbData.getKey().toString().trim();
			
			final Integer attbId = this.objectAttbsPersistenceInfo.get(attbName);
			final String attbValue = persistableString(currentAttbData.getValue().toString());
			
			try (final PreparedStatement hasAttbSt = con.prepareStatement("insert into has_attb(object_id,attribute_id,value) values (?,?,?)")){
				
				hasAttbSt.setInt(1, objectId);
				hasAttbSt.setInt(2, attbId);				
				hasAttbSt.setString(3, attbValue);
				
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
	
	private Map<String, Integer> loadAttbsPersistenceInfo(final Connection con, final SortedMapWritable record) throws SQLException {
		
		final Map<String, Integer> attbsPersistenceInfo = new TreeMap<>();
		
		for (@SuppressWarnings("rawtypes") final WritableComparable currentAttb : record.keySet()){
			
			final String attbName = currentAttb.toString();
			final Integer fieldId = insertAttb(con, attbName);
			
			attbsPersistenceInfo.put(attbName, fieldId);
		}
				
		return attbsPersistenceInfo;
	}

	private Integer insertAttb(final Connection con, final String attbName) throws SQLException {
		
		try (final PreparedStatement insertAttbSt = con.prepareStatement("insert into attribute (name) values (?)", 1)){
			
			insertAttbSt.setString(1, attbName.trim());
			
			insertAttbSt.executeUpdate();
			
			return lastInsertId(insertAttbSt);						
		}	
	}

	private int insertObject(final Connection con, final String hdfsObjectId) throws SQLException{
		
		try (final PreparedStatement insertObjectSt = con.prepareStatement("insert into object (hadoop_id) values (?)", 1)){
			
			insertObjectSt.setString(1, hdfsObjectId);
			
			insertObjectSt.executeUpdate();
			
			return lastInsertId(insertObjectSt);						
		}	
		
	}

	private int lastInsertId(final PreparedStatement st) throws SQLException {
		
		final ResultSet keys = st.getGeneratedKeys();
		keys.next();
		
		return keys.getInt(1);		
	}

	private Connection connectToDatabase() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
		
		Class.forName(super.getPropertyFromConfiguration(databaseDriverProperty)).newInstance();
		
		final Connection con = DriverManager.getConnection(super.getPropertyFromConfiguration(databaseConnectionStringProperty));
		
		con.setAutoCommit(false);			
		
		return con;
	}		
	
	private void cleanUpDatabase(final Connection con) throws SQLException{
		
		final String[] toCleanUpTables = {"object", "attribute"};
		
		for (final String currentTableToCleanUp : toCleanUpTables){
			
			cleanUpTable(con, currentTableToCleanUp);
		}		
	}

	private void cleanUpTable(final Connection con, final String tableName) throws SQLException {
		
		try (final Statement st = con.createStatement()){
			st.execute(buildCleanUpSql(tableName));
		}
	}

	private String buildCleanUpSql(final String tableName) {
		return "truncate ".concat(tableName);
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
