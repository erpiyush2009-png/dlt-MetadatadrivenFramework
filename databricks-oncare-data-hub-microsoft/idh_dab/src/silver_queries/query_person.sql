		SELECT  p.CONCERNROLEID, 
cr.CREATIONDATE CREATION_DATETIME,
coalesce(cti_ctr.EN_DESC,'') COUNTRY_BIRTH_EN_DESC,
coalesce(cti_ctr.FR_DESC,'') COUNTRY_BIRTH_FR_DESC,	

coalesce(cti_er.EN_DESC,'') ETHNIC_ORIGIN_EN_DESC,
coalesce(cti_er.FR_DESC,'') ETHNIC_ORIGIN_FR_DESC,	

coalesce(cti_ert.EN_DESC,'') ETHNIC_ORIGIN_TAB_EN_DESC, 
coalesce(cti_ert.FR_DESC,'') ETHNIC_ORIGIN_TAB_FR_DESC,	

coalesce(cti_fl.EN_DESC,'') FIRST_LANGUAGE_EN_DESC,  
coalesce(cti_fl.FR_DESC,'') FIRST_LANGUAGE_FR_DESC,

coalesce(cti_fi.EN_DESC,'') FNIM_IDENTITY_EN_DESC,  
coalesce(cti_fi.FR_DESC,'') FNIM_IDENTITY_FR_DESC,

coalesce(cti_g.EN_DESC,'') GENDER_EN_DESC,  
coalesce(cti_g.FR_DESC,'') GENDER_FR_DESC,

coalesce(cti_ig.EN_DESC,'') INDIGENOUS_GROUP_EN_DESC,  
coalesce(cti_ig.FR_DESC,'') INDIGENOUS_GROUP_FR_DESC,

coalesce(cti_ig2.EN_DESC,'') INDIGENOUS_GROUP2_EN_DESC,  
coalesce(cti_ig2.FR_DESC,'') INDIGENOUS_GROUP2_FR_DESC,		

coalesce(cti_lr.EN_DESC,'') LIVING_OFF_RESERVE_EN_DESC, 
coalesce(cti_lr.FR_DESC,'') LIVING_OFF_RESERVE_FR_DESC,	

coalesce(cti_ms.EN_DESC,'') MARITAL_STATUS_EN_DESC,  
coalesce(cti_ms.FR_DESC,'') MARITAL_STATUS_FR_DESC,		

coalesce(cti_n.EN_DESC,'') NATIONALITY_EN_DESC,  
coalesce(cti_n.FR_DESC,'') NATIONALITY_FR_DESC,		

coalesce(cti_ns.EN_DESC,'') NATIVE_STATUS_EN_DESC,  
coalesce(cti_ns.FR_DESC,'') NATIVE_STATUS_FR_DESC,		

coalesce(cti_pt.EN_DESC,'') PERSON_TYPE_EN_DESC,  
coalesce(cti_pt.FR_DESC,'') PERSON_TYPE_FR_DESC,		

coalesce(cti_pg.EN_DESC,'') POPULATION_GROUP_EN_DESC,  
coalesce(cti_pg.FR_DESC,'') POPULATION_GROUP_FR_DESC,		

coalesce(cti_rt.EN_DESC,'') RELIGION_TAB_EN_DESC,  
coalesce(cti_rt.FR_DESC,'') RELIGION_TAB_FR_DESC,		

coalesce(cti_si.EN_DESC,'') SPECIAL_INTEREST_EN_DESC,  
coalesce(cti_si.FR_DESC,'') SPECIAL_INTEREST_FR_DESC
--p.Effective_Start_DT

from (
Select	p.CONCERNROLEID,
	COUNTRYOFBIRTH 
	GENDER, 
	RACE,
	SPECIALINTERESTCODE, 
	MARITALSTATUSCODE, 
	NATIONALITYCODE, 
	`TYPE` PERSONAL_TYPE, 
	DATEOFBIRTHVERIND, 
	DATEOFDEATHVERIND, 
	PRIMARYALTERNATEID, 
	COUNTRYOFBIRTH, 
	ETHNICORIGINCODE, 

	INDIGENOUSGROUPCODE,
	
	ETHINICORIGINTABCODES, 
	RELIGION, 
	ABORIGINALANCESTRYCODE, 
	NATIVESTATUSCODE, 

	LIVINGOFFRESERVECODE, 
	PROVIDERMEMBERROLE, 
	FIRSTLANGUAGE, 

	INDIGENOUSGROUPCODETWO
	
	--cast(p.__Start_At.op_ts as timestamp) Effective_Start_DT
	
From {bronze_database}.cpin_curam_person p
join {bronze_database}.cpin_curam_mcyspersonext pe on p.CONCERNROLEID  = pe.CONCERNROLEID 

union all

Select	pp.CONCERNROLEID,
	COUNTRYOFBIRTH 
	GENDER, 
	RACE,
	SPECIALINTERESTCODE, 
	MARITALSTATUSCODE, 
	NATIONALITYCODE, 
	`TYPE` PERSONAL_TYPE, 
	DATEOFBIRTHVERIND, 
	DATEOFDEATHVERIND, 
	PRIMARYALTERNATEID, 
	COUNTRYOFBIRTH, 
	ETHNICORIGINCODE, 

	INDIGENOUSGROUPCODE,
	
	ppe.ETHNICORIGINTABCODES ETHINICORIGINTABCODES, 
	RELIGION, 
	ABORIGINALANCESTRYCODE, 
	NATIVESTATUSCODE, 

	LIVINGOFFRESERVECODE, 
	null PROVIDERMEMBERROLE, 
	FIRSTLANGUAGE, 

	INDIGENOUSGROUPCODETWO
	
	--cast(pp.__Start_At.op_ts as timestamp) Effective_Start_DT
	
From {bronze_database}.cpin_curam_PROSPECTPERSON pp
left join {bronze_database}.cpin_curam_MCYSProspectPersonExt ppe on pp.CONCERNROLEID  = ppe.CONCERNROLEID 

) p

join {bronze_database}.cpin_curam_concernrole cr on p.CONCERNROLEID  = cr.CONCERNROLEID 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ctr on p.COUNTRYOFBIRTH = cti_ctr.code and cti_ctr.TABLENAME ='Country'

left join {bronze_database}.cpin_curam_codetableitem_flat cti_g on p.GENDER = cti_g.code and cti_g.TABLENAME ='Gender' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_er on p.ETHNICORIGINCODE = cti_er.code and cti_er.TABLENAME ='EthnicOrigin' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_ert on p.ETHINICORIGINTABCODES = cti_ert.code and cti_ert.TABLENAME ='EthnicOrigin' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_fl on p.FIRSTLANGUAGE = cti_fl.code and cti_fl.TABLENAME ='Language' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_fi on p.ABORIGINALANCESTRYCODE = cti_fi.code and cti_fi.TABLENAME ='MCYSAboriginalAncestry' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_ig on p.INDIGENOUSGROUPCODE = cti_ig.code and cti_ig.TABLENAME ='IndigenousGroupCode' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_ig2 on p.INDIGENOUSGROUPCODETWO = cti_ig2.code and cti_ig2.TABLENAME ='IndigenousGroupCode' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_lr on p.LIVINGOFFRESERVECODE = cti_lr.code and cti_lr.TABLENAME ='MCYSYesNoUnknown' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_ms on p.MARITALSTATUSCODE = cti_ms.code and cti_ms.TABLENAME ='MaritalStatus' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_n on p.NATIONALITYCODE = cti_n.code and cti_n.TABLENAME ='Nationality' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_ns on p.NATIVESTATUSCODE = cti_ns.code and cti_ns.TABLENAME ='MCYSNativeStatus' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_pt on p.PERSONAL_TYPE = cti_pt.code and cti_pt.TABLENAME ='PersonType' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_pg on p.RACE = cti_pg.code and cti_pg.TABLENAME ='RaceCode' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_rt on p.RELIGION = cti_rt.code and cti_rt.TABLENAME ='MCYSReligion' 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_si on p.SPECIALINTERESTCODE = cti_si.code and cti_si.TABLENAME ='SpecialInterest'
