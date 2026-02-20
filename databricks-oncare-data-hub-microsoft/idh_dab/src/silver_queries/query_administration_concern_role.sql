Select  
cast(acr.ADMINISTRATIONCONCERNROLEID as bigint) ADMINISTRATION_CONCERN_ROLE_KEY,
cast(ar.ADMINISTRATIONROLEID as bigint) CONCERN_ROLE_KEY,  
acr.ADMINISTRATIONCONCERNROLEID ADMINISTRATION_CONCERN_ROLE_ID, 
acr.ADMINISTRATIONROLEID ADMINISTRATION_ROLE_ID,
acr.CONCERNROLEID CONCERNROLE_ID,
acr.ENDDATE END_DATE,
acr.STARTDATE START_DATE,
acr.TYPECODE TYPE_CODE,
Coalesce(ctie_t.Description,'') TYPE_CODE_EN_DESC,
Coalesce(ctif_t.Description,'') TYPE_CODE_FR_DESC,            

ar.ORGOBJECTLINKID ORG_OBJECT_LINK_ID,
ar.STATUSCODE STATUS_CODE,
Coalesce(ctie_s.Description,'') STATUS_CODE_EN_DESC,
Coalesce(ctif_s.Description,'') STATUS_CODE_FR_DESC,        

ar.USERNAME USER_NAME

From {bronze_database}.cpin_curam_administrationconcernrole acr
left Join {bronze_database}.cpin_curam_administrationrole ar on acr.ADMINISTRATIONROLEID  = ar.ADMINISTRATIONROLEID
left Join {bronze_database}.cpin_curam_concernrole cr on acr.concernroleid  = cr.concernroleid
left join {bronze_database}.cpin_curam_codetableitem ctie_s on ar.STATUSCODE = ctie_s.code and ctie_s.TABLENAME ='AdminRoleStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_s on ar.STATUSCODE = ctif_s.code and ctif_s.TABLENAME ='AdminRoleStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_t on acr.TYPECODE = ctie_t.code and ctie_t.TABLENAME ='AdminConcernRoleType' and ctie_t.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_t on acr.TYPECODE = ctif_t.code and ctif_t.TABLENAME ='AdminConcernRoleType' and ctif_t.LOCALEIDENTIFIER = 'fr'