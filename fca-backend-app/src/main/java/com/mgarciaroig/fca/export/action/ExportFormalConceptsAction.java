package com.mgarciaroig.fca.export.action;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;
import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Action to export all formal concepts and attributes data from hadoop cluster to database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportFormalConceptsAction extends DatabaseExportActionBase {
		
	private static final String etlHdfsFormalContextHdfsPathProperty = "etlHdfsFormalContextHdfsPath";
	private static final String analysisDataPreparationHdfsPathProperty = "analysisDataPreparationHdfsPath";
	private static final String exportMergeConceptsHdfsPathProperty = "exportMergeConceptsHdfsPath";
	
	public static void main(String[] args) throws IOException, FCAAnalizerException{
		
		final ExportFormalConceptsAction action = new ExportFormalConceptsAction(args);
		
		action.run();
	}
		

	public ExportFormalConceptsAction(String[] args) throws IOException {
		super(args);		
	}

	@Override
	protected void run() throws FCAAnalizerException {
		
		try (final Connection con = connectToDatabase()){
		
			cleanUpDatabase(con);
			
			persistAttributes(con, buildFormalConceptsRepository());
			
			persistFormalConcepts(con);
			
			con.commit();
		
		} catch (InstantiationException | IllegalAccessException| ClassNotFoundException | SQLException | IOException rawError ) {
			
			throw new ExportFormalConceptsException(rawError);
		}
		
	}
	
	private void persistFormalConcepts(final Connection con) throws IOException, SQLException{
								
		final Configuration conf = super.getConfiguration();
		final Option filePath = SequenceFile.Reader.file(new Path(super.getPropertyFromConfiguration(exportMergeConceptsHdfsPathProperty)));	
				
		
		try (final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath)){
			
			final NullWritable key = NullWritable.get();
			final FormalConcept value = new FormalConcept();
			
			while (sequenceFileReader.next(key, value)){
								
				insertFormalConcept(con, value);				
			}						
		}
	}
	
	private void insertFormalConcept(final Connection con, final FormalConcept formalConcept) throws SQLException {
		
		final Collection<String> objects = formalConcept.getObjects();
		final Collection<String> attributes = formalConcept.getAttributes();
		
		int formalConceptId = formalConcept.hashCode();		
		int numObjects = objects.size();		
		int numAttbs = attributes.size();
		
		try (final PreparedStatement insertAttbSt = con.prepareStatement("insert into formal_concept (hash,num_objects,num_attbs) values (?,?,?)", 1)){
			
			insertAttbSt.setInt(1, formalConceptId);
			insertAttbSt.setInt(2, numObjects);
			insertAttbSt.setInt(3, numAttbs);
			
			insertAttbSt.executeUpdate();													
		}
		
		addObjects(con, formalConceptId, objects);
		addAttributes(con, formalConceptId, attributes);		
	}
	
	private void addObjects(final Connection con, final int formalConceptId, final Collection<String> objects) throws SQLException {
		
		for (final String currentObject : objects){
			
			addObject(con, formalConceptId, currentObject);			
		}	
	}

	private void addObject(final Connection con, final int formalConceptId, final String hadoopObjectId) throws SQLException {
		
		String sqlStatement = "insert into has_object (formal_concept_id,object_id) values (?,(select id from object where hadoop_id =?))";
		
		try (final PreparedStatement insertObjectSt = con.prepareStatement(sqlStatement)){
			
			insertObjectSt.setInt(1, formalConceptId);
			insertObjectSt.setString(2, hadoopObjectId);
			
			insertObjectSt.executeUpdate();				
		}
	}

	private void addAttributes(final Connection con, final int formalConceptId, final Collection<String> attributes) throws SQLException {
		
		for (final String currentAtribute : attributes){
			
			addAttribute(con, formalConceptId, currentAtribute);			
		}
	}	


	private void addAttribute(final Connection con, final int formalConceptId, final String attribute) throws SQLException {
						
		try (final PreparedStatement insertAttributeSt = con.prepareStatement("insert into has_attb (formal_concept_id,attb_id) values (?,(select id from attribute where name = ?))")){
			
			insertAttributeSt.setInt(1, formalConceptId);
			insertAttributeSt.setString(2, attribute);
			
			insertAttributeSt.executeUpdate();				
		}		
	}

	private void persistAttributes(final Connection con, final FormalContextItemRepository formalConceptsRepository) 
			throws IOException, SQLException
	{
				
		for (final String currentAttb : formalConceptsRepository.findAllAttbs()){
			insertAttb(con, currentAttb);
		}
				
	}
	
	private int insertAttb(final Connection con, final String attbName) throws SQLException{
		try (final PreparedStatement insertAttbSt = con.prepareStatement("insert into attribute (name) values (?)", 1)){
			
			insertAttbSt.setString(1, attbName.trim());
			
			insertAttbSt.executeUpdate();
			
			return lastInsertId(insertAttbSt);						
		}	
	}		
	
	private void cleanUpDatabase(final Connection con) throws SQLException{
		
		final String[] toCleanUpTables = {"formal_concept", "attribute"};
		
		super.cleanUpDatabase(con, toCleanUpTables);		
	}	
	
	private FormalContextItemRepository buildFormalConceptsRepository(){
						
		final Configuration conf = super.getConfiguration();
		
		final Path formalContextPath = new Path(conf.getTrimmed(etlHdfsFormalContextHdfsPathProperty));
		final Path objectsIdsByAttributeBasePath = new Path(conf.getTrimmed(analysisDataPreparationHdfsPathProperty));
		
		return new FormalContextItemRepository(super.getFileSystem(), formalContextPath, objectsIdsByAttributeBasePath);			
	}

}
