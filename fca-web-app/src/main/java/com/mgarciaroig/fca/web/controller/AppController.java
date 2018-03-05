package com.mgarciaroig.fca.web.controller;

import java.io.IOException;
import java.sql.SQLException;

import com.mgarciaroig.fca.web.persistence.data.model.DomainObjectBuilder;
import com.mgarciaroig.fca.web.persistence.data.rawmodel.Object;
import com.mgarciaroig.fca.web.persistence.data.repo.ObjectRepository;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class AppController {
		
	private static final String VIEW_INDEX = "index";
	private static final String OBJECT_INDEX = "object";
	
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(AppController.class);
	
	@Autowired
	private ObjectRepository objectRepo;
	
	@Autowired
    DomainObjectBuilder objectBuilder;
		
	Iterable<Object> allObjects;

	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String welcome(ModelMap model) {				
											
		addObjectListToView(model);					
		
		return VIEW_INDEX;
	}
	
	@Transactional
	@RequestMapping(value = "/object/{hadoopObjectId}", method = RequestMethod.GET)
	public String viewObject(final ModelMap model, @PathVariable String hadoopObjectId) throws IOException, SQLException{
																	
		addObjectListToView(model);
				
		model.addAttribute("object", objectBuilder.build(hadoopObjectId));
		
		return OBJECT_INDEX;
	}
	
	private void addObjectListToView(final ModelMap model){
		model.addAttribute("allObjects", findAllObjects());
	}

	private Iterable<Object> findAllObjects() {
		
		if (this.allObjects == null){
			this.allObjects = objectRepo.findAll();
		}
		
		return this.allObjects;
	}	
}
