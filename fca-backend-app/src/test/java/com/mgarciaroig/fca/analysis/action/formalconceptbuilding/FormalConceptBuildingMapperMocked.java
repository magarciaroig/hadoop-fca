package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.analysis.model.AttributesUtil;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

class FormalConceptBuildingMapperMocked extends FormalConceptBuildingMapper{
	
	FormalConceptBuildingMapperMocked(final FormalContextItemRepository mockedRepository) throws IOException{
		
		this.repository = mockedRepository;
		this.attributesUtil = new AttributesUtil(this.repository.findAllAttbs());
	}
	
	@Override
	public void setup(final Context context) throws IOException{
	}

}
