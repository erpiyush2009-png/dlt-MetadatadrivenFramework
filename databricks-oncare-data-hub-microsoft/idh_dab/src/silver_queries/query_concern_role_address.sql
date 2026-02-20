SELECT 
cast(cra.CONCERNROLEADDRESSID as bigint) AS CONCERN_ROLE_ADDRESS_ID,
cast(cra.ADDRESSID as bigint)           AS ADDRESS_ID,
cast(cra.CONCERNROLEID as bigint)       AS CONCERN_ROLE_ID,
cast(cra.CONCERNROLEADDRESSID as bigint)     AS CONCERN_ROLE_KEY,
cra.TYPECODE                            AS ADDRESS_TYPE_CODE,
coalesce(cti_type_en.DESCRIPTION,'')    AS ADDRESS_TYPE_EN_DESC,
coalesce(cti_type_fr.DESCRIPTION,'')    AS ADDRESS_TYPE_FR_DESC,
cra.COMMENTS                            AS COMMENTS_TEXT,
cra.STATUSCODE                          AS STATUS_CODE,
coalesce(cti_status_en.DESCRIPTION,'')  AS STATUS_EN_DESC,
coalesce(cti_status_fr.DESCRIPTION,'')  AS STATUS_FR_DESC,
cast(cra.STARTDATE as timestamp)        AS START_DATE,
cast(cra.ENDDATE as timestamp)          AS END_DATE

FROM {bronze_database}.cpin_curam_concernroleaddress cra
LEFT JOIN {bronze_database}.cpin_curam_codetableitem cti_type_en 
ON cra.TYPECODE = cti_type_en.code 
AND cti_type_en.tablename = 'ConcernRoleAddress'
AND cti_type_en.LOCALEIDENTIFIER = 'en'

LEFT JOIN {bronze_database}.cpin_curam_codetableitem cti_type_fr 
ON cra.TYPECODE = cti_type_fr.code 
AND cti_type_fr.tablename = 'ConcernRoleAddress'
AND cti_type_fr.LOCALEIDENTIFIER = 'fr'

-- Join to CodeTableItem for StatusCode (EN/FR)
LEFT JOIN {bronze_database}.cpin_curam_codetableitem cti_status_en 
ON cra.STATUSCODE = cti_status_en.code 
AND cti_status_en.tablename = 'ConcernRoleAddress'
AND cti_status_en.LOCALEIDENTIFIER = 'en'

LEFT JOIN {bronze_database}.cpin_curam_codetableitem cti_status_fr 
ON cra.STATUSCODE = cti_status_fr.code 
AND cti_status_fr.tablename = 'ConcernRoleAddress'
AND cti_status_fr.LOCALEIDENTIFIER = 'fr';
