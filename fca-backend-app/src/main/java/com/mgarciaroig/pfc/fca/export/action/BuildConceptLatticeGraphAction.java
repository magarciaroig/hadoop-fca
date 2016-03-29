package com.mgarciaroig.pfc.fca.export.action;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;

/**
 * Action to build the concept lattice hierarchy over the exported formal concepts in the database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class BuildConceptLatticeGraphAction extends DatabaseExportActionBase {
			

	public static void main(String[] args) throws IOException, FCAAnalizerException {
		
		checkArguments(args);
		
		String jdbcDriverName = args[0];
		String connectionString = args[1];
		String user = args[2];
		String password = args[3];
				
				
		final BuildConceptLatticeGraphAction action = new BuildConceptLatticeGraphAction(args);				
				
		final Configuration config = action.getConfiguration();
		config.set("jdbc.driver.name", jdbcDriverName);
		config.set("jdbc.connection.string", connectionString);					
		config.set("user", user);
		config.set("password", password);
																								
		action.run();		
	}	
		
	BuildConceptLatticeGraphAction(String[] args) throws IOException {
		super(args);
	}

	@Override
	protected void run() throws FCAAnalizerException {
				
		
		try (final Connection con = connectToDatabase()){
												
			cleanUpDatabase(con);
			placeConceptsInLattice(con);
			
			con.commit();			
		}	
		catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException actionError){
			
			throw new BuildConceptLatticeGraphException(actionError);
		}
	}
			
	private void placeConceptsInLattice(final Connection con) throws SQLException{
		
		final TreeSet<String> allAttributes = loadAllAttributes(con);
		final Set<FormalConcept> leafConcepts = new HashSet<>();		
		
		int currentConceptLevel = 0;
		
		final Map<FormalConcept,Collection<FormalConcept>> childrenAndParents = new HashMap<>();
				
		for (int currentConceptSize = 0; currentConceptSize <= maxConceptsSize(allAttributes); currentConceptSize++){
			
			childrenAndParents.clear();
			
			final Collection<FormalConcept> pendingConcepts = pendingConcepts(con, currentConceptSize);
			
			for (final FormalConcept currentPendingConcept : pendingConcepts){
												
				if (currentConceptSize == 0){
					// We need to initialize the leaf concepts set with the root node
					leafConcepts.add(currentPendingConcept);
				}
				
				final Collection<FormalConcept> parents = parentConceptsFor(con, leafConcepts, currentPendingConcept);
				
				childrenAndParents.put(currentPendingConcept, parents);				
			} 
			
			if (!pendingConcepts.isEmpty()){
			
				setParenthood(con, childrenAndParents, leafConcepts);
			
				markAsProcessed(con, pendingConcepts, currentConceptLevel++);
			}
			
			con.commit();
		}
	}
	
	private void markAsProcessed(final Connection con, final Collection<FormalConcept> pendingConcepts, final int level) throws SQLException {
		
		final Integer[] conceptIds = new Integer[pendingConcepts.size()];
		
		int conceptIndex = 0;
		for (final FormalConcept currentConcept : pendingConcepts){
			conceptIds[conceptIndex++] = currentConcept.hashCode();
		}
		
		updateFormalConceptLevels(con, level, conceptIds);		
	}

	private void updateFormalConceptLevels(final Connection con, final int level, final Integer... conceptIds) throws SQLException {
				
		final String sql = String.format("update formal_concept set level = ? where hash in (%s)", StringUtils.join(conceptIds, ","));
		
		try (final PreparedStatement stLevelUpdater = con.prepareStatement(sql)){
			
			stLevelUpdater.setInt(1, level);
			
			stLevelUpdater.executeUpdate();
		}
		
	}

	private void setParenthood(final Connection con, final Map<FormalConcept, Collection<FormalConcept>> childrenAndParents, 
			final Set<FormalConcept> leafConcepts) throws SQLException 
	{	
		final Map<Integer,Integer[]> parenthoodInsertData = new HashMap<>();
		
		for (final Entry<FormalConcept, Collection<FormalConcept>> currentParentHoodEntry : childrenAndParents.entrySet()){
			
			final FormalConcept toBeAdded = currentParentHoodEntry.getKey();			
			final Collection<FormalConcept> parents = currentParentHoodEntry.getValue();
			
			updateLeafConcepts (toBeAdded, parents, leafConcepts);
			
			final Integer[] parentIds = getConceptIds(parents); 
			
			if ((parentIds != null) && (parentIds.length > 0)){
				parenthoodInsertData.put(toBeAdded.hashCode(), parentIds);
			}
		}
		
		if (parenthoodInsertData.size() > 0){
			insertParentsBySteps(con, parenthoodInsertData);
		}
	}

	private void updateLeafConcepts(final FormalConcept toBeAdded, final Collection<FormalConcept> parents, final Set<FormalConcept> leafConcepts) {
		
		if (parents.size() > 0){
			
			leafConcepts.add(toBeAdded);
								
			for (final FormalConcept currentParent : parents){								
				
				leafConcepts.remove(currentParent);				
			}						
		}						
	}	
	
	private void insertParentsBySteps(final Connection con, final Map<Integer,Integer[]> parenthoodInsertData) throws SQLException {
		
		for (final Entry<Integer, Integer[]> currentInsertData : parenthoodInsertData.entrySet()){
			
			final int currentConceptId = currentInsertData.getKey();
			final Integer[] currentParentIds = currentInsertData.getValue();
			
			insertparents(con, currentConceptId, currentParentIds);
		}
		
	}
	
	private void insertparents(final Connection con, final int conceptId, final Integer[] parentIds) throws SQLException{
		
		final StringBuilder insertSql = new StringBuilder("insert into formal_concept_hierarchy (formal_concept_id,parent_id) values ");
		
		final List<String> insertItems = new ArrayList<>(parentIds.length);
		
		for (final int currentParentId : parentIds){
			insertItems.add(String.format("(%d,%d)", conceptId, currentParentId));
		}	
		
		insertSql.append(StringUtils.join(insertItems, ","));
		
		try (Statement stInsert = con.createStatement()){
			stInsert.executeUpdate(insertSql.toString());
		}
		catch (SQLException error){
			System.err.print("Error launching insert sentence: ");
			System.err.println(insertSql);
			
			throw error;
		}
	}	

	private Collection<FormalConcept> parentConceptsFor(final Connection con, final Set<FormalConcept> leafConcepts, 
			final FormalConcept toBeInserted) throws SQLException 
	{
				
		Collection<FormalConcept> parents = filterConceptsICanInheritFrom(leafConcepts, toBeInserted);
						
		// If no parent from leaf nodes is valid, lookup for its ancestors in the hierarchy
		if (parents.isEmpty()){
			parents = closestAncestorConceptsToInheritFrom(con, leafConcepts, toBeInserted);
		}
		
		return parents;
	}
	
	private Collection<FormalConcept> closestAncestorConceptsToInheritFrom(final Connection con, final Collection<FormalConcept> children, 
			final FormalConcept descendant) throws SQLException 
	{		
		if (children.isEmpty()){
			return new ArrayList<>();
		}
		
		final Collection<FormalConcept> parents = loadParents(con, getConceptIds(children));						
		final Collection<FormalConcept> parentsICanInheritFrom = filterConceptsICanInheritFrom(parents, descendant);
						
		if (parentsICanInheritFrom.isEmpty()){
			return closestAncestorConceptsToInheritFrom(con, parents, descendant);
		}		
		
		return parentsICanInheritFrom;
	}
	
	private Collection<FormalConcept> filterConceptsICanInheritFrom(final Collection<FormalConcept> parents, final FormalConcept child){
		
		final Collection<FormalConcept> parentsICanInheritFrom = new ArrayList<>();
		
		for (final FormalConcept currentParent : parents){
			
			if (includedIn(currentParent, child)){
				parentsICanInheritFrom.add(currentParent);
			}
		}
		
		return parentsICanInheritFrom;
	}
	
	private Collection<FormalConcept> loadParents(final Connection con, final Integer... childConceptIds) throws SQLException {
		
		final String sql = String.format("select distinct parent_id from formal_concept_hierarchy where formal_concept_id in (%s)", 
				StringUtils.join(childConceptIds, ","));
		
		final List<Integer> parentIds = new ArrayList<>();
		
		try (final Statement stParentFinder = con.createStatement()){
			
			final ResultSet rsParentFinder = stParentFinder.executeQuery(sql);
			
			while (rsParentFinder.next()){
				parentIds.add(rsParentFinder.getInt(1));
			}
			
			rsParentFinder.close();
		}
		
		return loadFormalConcepts(con, parentIds.toArray(new Integer[]{}));		
	}
	
	private Integer[] getConceptIds(final Collection<FormalConcept> concepts){
		
		Integer[] conceptIds = new Integer[concepts.size()];
		
		int conceptIndex = 0;
		for (final FormalConcept currentConcept : concepts){
			
			conceptIds[conceptIndex++] = currentConcept.hashCode();
		}
		
		return conceptIds;
	}

	private boolean includedIn(final FormalConcept leafConcept, final FormalConcept pendingConcept){
		
		final TreeSet<String> pendingConceptAttbs = pendingConcept.getAttributes();				
		final TreeSet<String> leafConceptAttbs = leafConcept.getAttributes();
		
		return leafConceptAttbs.size() < pendingConceptAttbs.size() && pendingConceptAttbs.containsAll(leafConceptAttbs);					
	}

	private Collection<FormalConcept> pendingConcepts(final Connection con, final int conceptSize) throws SQLException{
		
		final Integer[] pendingConceptIds = findPendingFormalConceptIdsBySize(con, conceptSize);
		
		return loadFormalConcepts(con, pendingConceptIds);
	}	
	
	private Integer[] findPendingFormalConceptIdsBySize(final Connection con, final int conceptSize) throws SQLException{
		
		final List<Integer> ids = new ArrayList<>();
		
		final String query = "select hash from formal_concept where level is null and num_attbs = ?";
		
		try (final PreparedStatement stFormalConceptFinder = con.prepareStatement(query)){
			
			stFormalConceptFinder.setInt(1, conceptSize);
			
			final ResultSet rsFormalConceptFinder = stFormalConceptFinder.executeQuery();
			
			while (rsFormalConceptFinder.next()){
				ids.add(rsFormalConceptFinder.getInt(1));
			}
			
			rsFormalConceptFinder.close();
		}
		
		return ids.toArray(new Integer[]{});
	}
	
	private Collection<FormalConcept> loadFormalConcepts(final Connection con, final Integer... formalConceptIds) throws SQLException{
		
		final List<FormalConcept> concepts = new ArrayList<>();
		
		if (formalConceptIds.length < 1){
			return concepts;
		}
		
		for (final int currentId : formalConceptIds){
			concepts.add(loadFormalConcept(con,currentId));
		}		
		
		return concepts;				
	}
	
	private FormalConcept loadFormalConcept(final Connection con, final int formalConceptId) throws SQLException {
	
		final Map<Integer,List<String>> objectsData = loadFormalConceptObjects(con, formalConceptId);
		final Map<Integer,List<String>> attbsData = loadFormalConceptAttbs(con, formalConceptId);
		
		List<String> objects = objectsData.get(formalConceptId);
		if (objects == null){
			objects = new ArrayList<>();
		}
		
		List<String> attbs = attbsData.get(formalConceptId);
		if (attbs == null){
			attbs = new ArrayList<>();
		}				
		
		return new FormalConcept(objects,attbs);
	}
	
	private Map<Integer,List<String>> loadFormalConceptObjects(final Connection con, final Integer... formalConceptIds) throws SQLException {
									
		final StringBuilder objectsQueryBuilder = new StringBuilder("select distinct ho.formal_concept_id,o.hadoop_id ");				
		objectsQueryBuilder.append("from has_object ho inner join object o on o.id = ho.object_id ");	
		objectsQueryBuilder.append("where ho.formal_concept_id = ?");			
		
		return loadFormalConceptOrderedDerivedData(con, objectsQueryBuilder.toString(), formalConceptIds);
		
	}
	
	private Map<Integer,List<String>> loadFormalConceptAttbs(final Connection con, final Integer... formalConceptIds) throws SQLException {
						
		final StringBuilder attbsQueryBuilder = new StringBuilder("select distinct ha.formal_concept_id,a.name ");		
		attbsQueryBuilder.append("from has_attb ha inner join attribute a on a.id = ha.attb_id ");		
		attbsQueryBuilder.append("where ha.formal_concept_id = ?");		
		
		return loadFormalConceptOrderedDerivedData(con, attbsQueryBuilder.toString(), formalConceptIds);
	}
	
	private Map<Integer,List<String>> loadFormalConceptOrderedDerivedData(final Connection con, final String sqlParametrizedQuery, final Integer... formalConceptIds) throws SQLException {
		
		final Map<Integer,List<String>> derivedData = new HashMap<>();
		
		final String stringfiedIds = StringUtils.join(formalConceptIds, ",");
		final String finalQuery = sqlParametrizedQuery.replace("?", stringfiedIds);
						
		try (final Statement stDerivedDataReader = con.createStatement()){
									
			final ResultSet rsDerivedDataReader = stDerivedDataReader.executeQuery(finalQuery);
			
			while (rsDerivedDataReader.next()){
				
				final int conceptId = rsDerivedDataReader.getInt(1);
				final String derivedString = rsDerivedDataReader.getString(2);
				
				List<String> derivedStrings =  derivedData.get(conceptId);
				if (derivedStrings == null){
					derivedStrings = new ArrayList<String>();
					derivedData.put(conceptId, derivedStrings);
				}
				
				derivedStrings.add(derivedString);
			}
			
			rsDerivedDataReader.close();			
		}
		catch (java.lang.OutOfMemoryError err){
			
			System.err.print("Error launching query: ");
			System.err.println(finalQuery);
			
			throw err;
		}
		
		return derivedData;
		
	}

	private int maxConceptsSize(final TreeSet<String> allAttributes) {
		return allAttributes.size();
	}
	
	private TreeSet<String> loadAllAttributes(final Connection con) throws SQLException {
		
		final TreeSet<String> allAttbs = new TreeSet<>();
		
		try (final Statement stReadAttbs = con.createStatement()){
			
			final ResultSet rsReadAttbs = stReadAttbs.executeQuery("select name from attribute");
			
			while (rsReadAttbs.next()){
				allAttbs.add(rsReadAttbs.getString(1));
			}
			
			rsReadAttbs.close();
		}
		
		return allAttbs;
	}
	
	private void cleanUpDatabase(final Connection con) throws SQLException {
		
		super.cleanUpDatabase(con, "formal_concept_hierarchy");
		
		resetFormalConceptsLevel(con);		
	}
	
	private void resetFormalConceptsLevel(final Connection con) throws SQLException {
		
		final Statement stResetFormalConceptLevels = con.createStatement();
		stResetFormalConceptLevels.executeUpdate("update formal_concept set level = NULL");
	}
	
	private static void checkArguments(String[] args) {
		
		if (args.length != 4){
			
			String className = BuildConceptLatticeGraphAction.class.getName();
			
			System.err.printf("Usage: java %s [jdbc.driver.name] [jdbc.connection.string] [user] [password]\n", className);
			System.err.printf("For example: java %s com.mysql.jdbc.Driver jdbc:mysql://localhost/fca?user=fca&password=fca fca fca\n", className);
			
			System.exit(1);
		}
	}
}
