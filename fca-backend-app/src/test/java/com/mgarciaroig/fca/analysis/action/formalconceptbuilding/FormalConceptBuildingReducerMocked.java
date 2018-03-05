package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.analysis.model.AttributesUtil;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

class FormalConceptBuildingReducerMocked extends FormalConceptBuildingReducer {
	
	FormalConceptBuildingReducerMocked(final FormalContextItemRepository mockedRepository) throws IOException{
				
		this.attributesUtil = new AttributesUtil(mockedRepository.findAllAttbs());
	}
	
	@Override
	public void setup(final Context context) throws IOException{
	}

}
