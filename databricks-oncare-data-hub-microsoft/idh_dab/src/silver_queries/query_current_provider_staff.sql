Select 
	p.providerConcernRoleID AS PROVIDER_CONCERN_ROLE_ID,
	ol.ORGOBJECTLINKID AS ORG_OBJECT_LINK_ID,
	u.username AS USERNAME ,
	u.firstname AS FIRST_NAME,
	u.fullName AS FULL_NAME,
	u.surname AS LAST_NAME,
	coalesce(cti_ar.EN_DESC, '')  AS ROLE_TYPE_DESC_EN ,
	coalesce(cti_ar.FR_DESC, '')  AS ROLE_TYPE_DESC_FR,
	ou.name AS AGENCY,
	ou.organisationunitid AS ORGANISATION_UNIT_ID

From {bronze_database}.cpin_curam_provider p
Join {bronze_database}.cpin_curam_ADMINISTRATIONCONCERNROLE acr on p.PROVIDERCONCERNROLEID = acr.CONCERNROLEID
Join {bronze_database}.cpin_curam_ADMINISTRATIONROLE ar on acr.ADMINISTRATIONROLEID  = ar.ADMINISTRATIONROLEID
Join {bronze_database}.cpin_curam_users u on ar.USERNAME  = u.USERNAME
Join {bronze_database}.cpin_curam_orgobjectlink ol on u.USERNAME  = ol.USERNAME
Join {bronze_database}.cpin_curam_organisationunit ou on u.LOCATIONID  = ou.LOCATIONID 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ar on acr.TYPECODE = cti_ar.code and cti_ar.TABLENAME ='AdminConcernRoleType' 
Where  acr.TYPECODE  in ( 'AC1', 'AC2', 'AC13000' ) AND ar.STATUSCODE = 'AS1' --- Active Status
