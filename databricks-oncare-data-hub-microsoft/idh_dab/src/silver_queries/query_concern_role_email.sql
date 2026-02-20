Select  cre.CONCERNROLEEMAILADDRESSID CR_EMAIL_ADDRESS_ID,
cre.EMAILADDRESSID EMAIL_ADDRESS_ID,
cre.CONCERNROLEID CONCERN_ROLE_ID,
ea.COMMENTS COMMENT_TEXT,
ea.EMAILADDRESS EMAIL_ADDRESS_TEXT,
ea.STATUSCODE STATUS_CODE,
coalesce(ctie_s.DESCRIPTION,'') STATUS_EN_DESC,
coalesce(ctif_s.DESCRIPTION,'') STATUS_FR_DESC,
cre.TYPECODE TYPE_CODE,
coalesce(ctie_t.DESCRIPTION,'') TYPE_EN_DESC,
coalesce(ctif_t.DESCRIPTION,'') TYPE_FR_DESC
From {bronze_database}.cpin_curam_concernroleemailaddress cre
join {bronze_database}.cpin_curam_emailaddress ea on cre.EMAILADDRESSID  = ea.EMAILADDRESSID
left join {bronze_database}.cpin_curam_codetableitem ctie_s on ea.STATUSCODE = ctie_s.code and ctie_s.TABLENAME ='RecordStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_s on ea.STATUSCODE = ctif_s.code and ctif_s.TABLENAME ='RecordStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_t on cre.TYPECODE = ctie_t.code and ctie_t.TABLENAME ='RecordStatus' and ctie_t.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_t on cre.TYPECODE = ctif_t.code and ctif_t.TABLENAME ='RecordStatus' and ctif_t.LOCALEIDENTIFIER = 'fr'
