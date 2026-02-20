SELECT
    ou.ORGANISATIONUNITID  ORGANISATION_UNIT_ID,
    ou.name AS ORGANISATION_NAME,
    l.ADDRESSID  ADDRESS_ID ,
    ou.STATUSCODE  STATUS_CODE ,
	coalesce(cti_s.EN_DESC, '') STATUS_EN_DESC,
	coalesce(cti_s.FR_DESC, '') STATUS_FR_DESC,
    ou.RECORDSTATUS  RECORD_STATUS_CODE,
	coalesce(cti_rs.EN_DESC, '') RECORD_STATUS_EN_DESC,
	coalesce(cti_rs.FR_DESC, '') RECORD_STATUS_FR_DESC        
FROM {bronze_database}.cpin_curam_organisationunit ou
JOIN {bronze_database}.cpin_curam_location l on ou.locationid = l.LOCATIONID
left join {bronze_database}.cpin_curam_codetableitem_flat cti_s on ou.RECORDSTATUS = cti_s.code and cti_s.TABLENAME ='OrganisationUnit'
left join {bronze_database}.cpin_curam_codetableitem_flat cti_rs on ou.statuscode = cti_rs.code and cti_rs.TABLENAME ='OrganisationUnit'