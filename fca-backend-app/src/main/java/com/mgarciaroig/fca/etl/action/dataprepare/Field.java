package com.mgarciaroig.fca.etl.action.dataprepare;

import com.mgarciaroig.fca.etl.action.dataconvert.FieldType;

/**
 * ETL field abstraction
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public enum Field {
				
	Location_country(FieldGroup.LOCATION, "country"),
	Location_city(FieldGroup.LOCATION, "city"),
	
	Facility_name(FieldGroup.FACILITY, "name"),
	Facility_IAEA_code(FieldGroup.FACILITY, "IAEA_code"),
	
	Status_status(FieldGroup.STATUS, "status"),
	Status_comment(FieldGroup.STATUS, "comment"),
	
	Category(FieldGroup.CATEGORY, "category"),
	
	Information_date(FieldGroup.INFORMATION, "date", FieldType.DATE_FIELD, 4),
	Information_owner(FieldGroup.INFORMATION, "owner"),
	Information_operator(FieldGroup.INFORMATION, "operator"),
	Information_licensing(FieldGroup.INFORMATION, "licensing"),
	Information_administrator(FieldGroup.INFORMATION, "administrator"),
	Information_safeguards(FieldGroup.INFORMATION, "safeguards"),
	Information_construction(FieldGroup.INFORMATION, "construction", FieldType.DATE_FIELD, 4),
	Information_criticality(FieldGroup.INFORMATION, "criticality", FieldType.DATE_FIELD, 4),
	Information_operators(FieldGroup.INFORMATION, "operators", FieldType.LONG_FIELD, 3),
	Information_total_staff(FieldGroup.INFORMATION, "total_staff", FieldType.LONG_FIELD, 3),
	
	TechData_reactor_type(FieldGroup.TECHDATA, "reactor_type"),
	TechData_thermal_power_steady_kw(FieldGroup.TECHDATA, "thermal_power_steady_kw", FieldType.DOUBLE_FIELD, 3),
	TechData_thermal_power_pulsed_mw(FieldGroup.TECHDATA, "thermal_power_pulsed_mw", FieldType.DOUBLE_FIELD, 3),
	TechData_max_flux_steady_thermal(FieldGroup.TECHDATA, "max_flux_steady_thermal", FieldType.DOUBLE_FIELD, 3),
	TechData_max_flux_steady_fast(FieldGroup.TECHDATA, "max_flux_steady_fast", FieldType.DOUBLE_FIELD, 3),
	TechData_max_flux_pulsed_thermal(FieldGroup.TECHDATA, "max_flux_pulsed_thermal", FieldType.DOUBLE_FIELD, 3),
	TechData_max_flux_pulsed_fast(FieldGroup.TECHDATA, "max_flux_pulsed_fast", FieldType.DOUBLE_FIELD, 3),
	TechData_moderator_material(FieldGroup.TECHDATA, "moderator_material"),
	TechData_coolant_material(FieldGroup.TECHDATA, "coolant_material"),
	TechData_control_rods_material(FieldGroup.TECHDATA, "control_rods_material"),
	TechData_control_rods_number(FieldGroup.TECHDATA, "control_rods_number", FieldType.LONG_FIELD, 3),
	TechData_reflector_material(FieldGroup.TECHDATA, "reflector_material"),
	TechData_sites_number(FieldGroup.TECHDATA, "sites_number", FieldType.LONG_FIELD, 3),
	TechData_cooling_natural_convection(FieldGroup.TECHDATA, "cooling_natural_convection", FieldType.BOOLEAN_FIELD),
	TechData_cooling_forced(FieldGroup.TECHDATA, "cooling_forced", FieldType.BOOLEAN_FIELD),
	
	Experimental_vertical_channels(FieldGroup.EXPERIMENTAL, "vertical_channels", FieldType.LONG_FIELD, 3),
	Experimental_vertical_max_flux(FieldGroup.EXPERIMENTAL, "vertical_max_flux", FieldType.DOUBLE_FIELD, 3),
	Experimental_horizontal_channels(FieldGroup.EXPERIMENTAL, "horizontal_channels", FieldType.LONG_FIELD, 3),
	Experimental_horizontal_max_flux(FieldGroup.EXPERIMENTAL, "horizontal_max_flux", FieldType.DOUBLE_FIELD, 3),	
	Experimental_use(FieldGroup.EXPERIMENTAL, "use"),
	Experimental_incore_irradiation_facilities(FieldGroup.EXPERIMENTAL, "incore_irradiation_facilities", FieldType.LONG_FIELD, 3),
	Experimental_incore_max_flux(FieldGroup.EXPERIMENTAL, "incore_max_flux", FieldType.DOUBLE_FIELD, 3),
	Experimental_reflector_max_flux(FieldGroup.EXPERIMENTAL, "reflector_max_flux", FieldType.DOUBLE_FIELD, 3),
	Experimental_irradiation_number(FieldGroup.EXPERIMENTAL, "irradiation_number", FieldType.LONG_FIELD, 3),
	Experimental_irradiation_channels(FieldGroup.EXPERIMENTAL, "irradiation_channels", FieldType.DOUBLE_FIELD, 3),
	
	Utilization_hours_per_day(FieldGroup.UTILIZATION, "hours_per_day", FieldType.LONG_FIELD, 4),
	Utilization_days_per_week(FieldGroup.UTILIZATION, "days_per_week", FieldType.LONG_FIELD, 4),
	Utilization_weeks_per_year(FieldGroup.UTILIZATION, "weeks_per_year", FieldType.LONG_FIELD, 4),
	Utilization_mw_days_per_year(FieldGroup.UTILIZATION, "mw_days_per_year", FieldType.LONG_FIELD, 4),
	Utilization_materials(FieldGroup.UTILIZATION, "materials", FieldType.BOOLEAN_FIELD),
	Utilization_runs_number_per_year(FieldGroup.UTILIZATION, "runs_number_per_year", FieldType.LONG_FIELD, 3),
	Utilization_isotope_total_activity_per_year(FieldGroup.UTILIZATION, "isotope_total_activity_per_year", FieldType.LONG_FIELD, 3),
	Utilization_isotopes(FieldGroup.UTILIZATION, "isotopes"),
	Utilization_neutron_scattering_hours_per_year(FieldGroup.UTILIZATION, "neutron_scattering_hours_per_year", FieldType.LONG_FIELD, 4),
	Utilization_neutron_scattering_methods(FieldGroup.UTILIZATION, "neutron_scattering_methods"),
	Utilization_neutron_radiography(FieldGroup.UTILIZATION, "neutron_radiography", FieldType.BOOLEAN_FIELD),
	Utilization_neutron_radiography_hours_per_year(FieldGroup.UTILIZATION, "neutron_radiography_hours_per_year", FieldType.LONG_FIELD, 4),
	Utilization_neutron_capture_therapy(FieldGroup.UTILIZATION, "neutron_capture_therapy", FieldType.BOOLEAN_FIELD),
	Utilization_neutron_capture_therapy_patients_per_year(FieldGroup.UTILIZATION, "neutron_capture_therapy_patients_per_year", FieldType.LONG_FIELD, 4),
	Utilization_neutron_activation_analysis_samples_per_year(FieldGroup.UTILIZATION, "neutron_activation_analysis_samples_per_year", FieldType.LONG_FIELD, 3),
	Utilization_neutron_activation_analysis_methods(FieldGroup.UTILIZATION, "neutron_activation_analysis_methods"),
	Utilization_transmutation_mass_kg_per_year(FieldGroup.UTILIZATION, "transmutation_mass_kg_per_year", FieldType.LONG_FIELD, 3),
	Utilization_transmutation_gemstone_kg_per_year(FieldGroup.UTILIZATION, "transmutation_gemstone_kg_per_year", FieldType.LONG_FIELD, 3),
	Utilization_geochronology_samples_per_year(FieldGroup.UTILIZATION, "geochronology_samples_per_year", FieldType.LONG_FIELD, 3),
	Utilization_geochronology_methods(FieldGroup.UTILIZATION, "geochronology_methods"),
	Utilization_teaching(FieldGroup.UTILIZATION, "teaching", FieldType.BOOLEAN_FIELD),
	Utilization_teaching_students_year(FieldGroup.UTILIZATION, "teaching_students_year", FieldType.LONG_FIELD, 3),
	Utilization_training(FieldGroup.UTILIZATION, "training", FieldType.BOOLEAN_FIELD),	
	Utilization_experimenters_number(FieldGroup.UTILIZATION, "experimenters_number", FieldType.LONG_FIELD, 3),
	Utilization_other_use(FieldGroup.UTILIZATION, "other_use", FieldType.BOOLEAN_FIELD),
	Utilization_other_use_describe(FieldGroup.UTILIZATION, "other_use_describe");
	
	
	private final static String groupToFieldSeparator = ".";
			
	public static Field buildFieldFromStringfiedRepresentation(final String stringfiedField){
		
		Field searchedField = null;
		
		final String[] fieldParts = stringfiedField.split("\\".concat(groupToFieldSeparator));
		
		final int requiredFieldPartsNumber = 2;
		
		if (fieldParts.length == requiredFieldPartsNumber){
						
			final String searchedGroup = fieldParts[0];
			final String searchedCode = fieldParts[1];
			
			for (final Field currentField : values()){
				
				final boolean matchedCode = currentField.code.equals(searchedCode);		
				final boolean matchedGroup = currentField.group.toString().equals(searchedGroup);
				
				if (matchedCode && matchedGroup){
					searchedField = currentField;
					break;
				}
			}
		}
		
		return searchedField;
	}
	
	private final FieldGroup group;
	private final String code;
	private FieldType type;
	private final Integer kmeansKParameter;
	
	private Field(final FieldGroup group, final String code){
		
		this(group, code, FieldType.TEXT_FIELD);
	}
	
	private Field(final FieldGroup group, final String code, final FieldType type){
				
		this(group, code, type, null);
	}
	
	private Field(final FieldGroup group, final String code, final FieldType type, final Integer kmeansKParameter){
		
		this.group = group;
		this.code = code;
		this.type = type;
		this.kmeansKParameter = kmeansKParameter;
	}
	
	public FieldType getType(){
		return this.type;
	}
	
	public Integer getKmeansKParameter(){
		return kmeansKParameter;
	}
	
	@Override
	public String toString(){				
		
		final StringBuilder str = new StringBuilder(group.toString());		
		str.append(groupToFieldSeparator).append(code);
		
		return str.toString();
	}	
}
