
CREATE TEMP TABLE temp_drug_exposure
  (
      drug_exposure_id				BIGINT			 	NOT NULL ,
  person_id						BIGINT			 	NOT NULL ,
  drug_concept_id				INTEGER			  	NOT NULL ,
  drug_exposure_start_date		DATE			    NULL ,
  drug_exposure_start_datetime	TIMESTAMP		 	NOT NULL ,
  drug_exposure_end_date		DATE			    NULL ,
  drug_exposure_end_datetime	TIMESTAMP		  	NOT NULL ,
  verbatim_end_date				DATE			    NULL ,
  drug_type_concept_id			INTEGER			  	NOT NULL ,
  stop_reason					VARCHAR(20)			NULL ,
  refills						INTEGER		  		NULL ,
  quantity						NUMERIC			    NULL ,
  days_supply					INTEGER		  		NULL ,
  sig							TEXT				NULL ,
  route_concept_id				INTEGER				NOT NULL ,
  lot_number					VARCHAR(50)	 		NULL ,
  provider_id					BIGINT			  	NULL ,
  visit_occurrence_id			BIGINT			  	NULL ,
  visit_detail_id               BIGINT       		NULL ,
  drug_source_value				VARCHAR(50)	  		NULL ,
  drug_source_concept_id		INTEGER			  	NULL ,
  route_source_value			VARCHAR(50)	  		NULL ,
  dose_unit_source_value		VARCHAR(50)	  		NULL
  )
;

INSERT INTO temp_drug_exposure(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime,
							   drug_exposure_end_date, drug_exposure_end_datetime,drug_type_concept_id,
							  days_supply, route_concept_id, visit_occurrence_id, visit_detail_id, drug_source_value)
SELECT procedure_occurrence_id as drug_exposure_id,
		person_id as person_id,
		1397141 as drug_concept_id,
		procedure_date as drug_exposure_start_date,
		procedure_datetime as drug_exposure_start_datetime,
		procedure_date as drug_exposure_end_date,
		procedure_datetime as drug_exposure_end_datetime,
		38000179 as drug_type_concept_id,
		1 as days_supply,
		4302785 as route_concept_id,
		visit_occurrence_id as visit_occurrence_id,
		visit_detail_id as visit_detail_id,
		procedure_source_value as drug_source_value
FROM cdmpv531.procedure_occurrence where procedure_source_value = '';

SELECT COUNT(DRUG_EXPOSURE_ID) FROM @cdm_database_scheme.drug_exposure where drug_exposure_id in (select drug_exposure_id from temp_drug_exposure);
--0

INSERT INTO cdmpv531.drug_exposure(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime,
							   drug_exposure_end_date, drug_exposure_end_datetime,drug_type_concept_id,
							  days_supply, route_concept_id, visit_occurrence_id, visit_detail_id, drug_source_value)
SELECT drug_exposure_id,
		person_id,
		drug_concept_id,
		drug_exposure_start_date,
		drug_exposure_start_datetime,
		drug_exposure_end_date,
		drug_exposure_end_datetime,
		drug_type_concept_id,
		days_supply,
		route_concept_id,
		visit_occurrence_id,
		visit_detail_id,
		drug_source_value
FROM temp_drug_exposure;
