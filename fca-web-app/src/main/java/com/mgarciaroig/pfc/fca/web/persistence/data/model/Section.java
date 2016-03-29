package com.mgarciaroig.pfc.fca.web.persistence.data.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class Section implements Comparable<Section> {
		
	private final static int LESS_THAN_COMP_RET_CODE = -1;
	private final static int EQUAL_THAN_COMP_RET_CODE = 0;
	private final static int GREATER_THAN_COMP_RET_CODE = 1;
		
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(Section.class);
	
	
	private static List<String> mainSectionsInWeightOrder;
	
	static {
		
		try {
			
			mainSectionsInWeightOrder = mainSectionsInOrder();
		
		} catch (final IOException error) {
			
			logger.error("Error retrieving sections info", error);
			
			Throwables.propagate(error);
		}
	}
	
	private final String name;
	
	private Set<Field> fields = new TreeSet<>();
	
	Section(final String name){
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public List<Field> getFields() {
		return new ArrayList<>(fields);
	}
	
	void addField(final String name, final String value){
		fields.add(new Field(name, value));
	}

	@Override
	public int compareTo(final Section you) {
		
		final String myName = this.getName();
		final String yourName = you.getName();				
		
		boolean amIAMainSection = mainSectionsInWeightOrder.contains(myName); 
		boolean areYouAMainSection = mainSectionsInWeightOrder.contains(yourName);
		
		if (amIAMainSection && areYouAMainSection){
			return compareToWeighted(you);
		}
		else if (amIAMainSection && !areYouAMainSection){
									
			return LESS_THAN_COMP_RET_CODE;
		}
		else if (!amIAMainSection && areYouAMainSection){
									
			return GREATER_THAN_COMP_RET_CODE;
		}
		else {
			compareToLexically(you);
		}
		
		return 0;
	}	
	
	private int compareToLexically(final Section you){
		
		return getName().compareTo(you.getName());				
	}
	
	private int compareToWeighted(final Section you){
		
		int myOrder = mainSectionsInWeightOrder.indexOf(this.getName());
		int yourOrder = mainSectionsInWeightOrder.indexOf(you.getName());
		
		if (myOrder == yourOrder){
			return EQUAL_THAN_COMP_RET_CODE;
		}
		else if (myOrder < yourOrder){
			return LESS_THAN_COMP_RET_CODE;
		}
		else {
			return GREATER_THAN_COMP_RET_CODE;
		}				
	}	
	
	private static List<String> mainSectionsInOrder() throws IOException {
						
		return Arrays.asList(loadSectionProperties().getProperty("sectionsorder", "").split(","));
	}
	
	private static Properties loadSectionProperties() throws IOException{
		
		final Properties props = new Properties();
		
		props.load(DomainObject.class.getResourceAsStream("sectionorder.properties"));
		
		return props;
	}
}
