Select  
crp.CONCERNROLEID CONCERN_ROLE_ID,
crp.CONCERNROLEPHONENUMBERID CR_PHONE_NUMBER_ID,
crp.ENDDATE END_DATE,
p.COMMENTS COMMENTS_TEXT,
p.PHONEAREACODE PHONE_AREA_CODE,
p.PHONECOUNTRYCODE PHONE_COUNTRY_CODE,
p.PHONEEXTENSION PHONE_EXTENSION_NUM,
p.PHONENUMBER PHONE_NUMBER,
crp.PHONENUMBERID PHONE_NUMBER_ID,
crp.STARTDATE START_DATE,
p.STATUSCODE STATUS_CODE,
coalesce(ctie_s.DESCRIPTION,'') STATUS_EN_DESC,
coalesce(ctif_s.DESCRIPTION,'') STATUS_FR_DESC,		
crp.TYPECODE TYPE_CODE,
coalesce(ctie_t.DESCRIPTION,'') TYPE_EN_DESC,
coalesce(ctif_t.DESCRIPTION,'') TYPE_FR_DESC	
From {bronze_database}.cpin_curam_concernrolephonenumber crp
join {bronze_database}.cpin_curam_phonenumber p on crp.PHONENUMBERID  = p.PHONENUMBERID
left join {bronze_database}.cpin_curam_codetableitem ctie_s on p.STATUSCODE = ctie_s.code and ctie_s.TABLENAME ='RecordStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_s on p.STATUSCODE = ctif_s.code and ctif_s.TABLENAME ='RecordStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_t on crp.TYPECODE = ctie_t.code and ctie_t.TABLENAME ='PhoneType' and ctie_t.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_t on crp.TYPECODE = ctif_t.code and ctif_t.TABLENAME ='PhoneType' and ctif_t.LOCALEIDENTIFIER = 'fr'
