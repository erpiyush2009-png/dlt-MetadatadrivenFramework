Select  hash('spark', u.USERNAME,2) USER_NAME_KEY, 
Coalesce(ctie.DESCRIPTION,'') RECORD_STATUS_EN_DESC, 
Coalesce(ctif.DESCRIPTION,'') RECORD_STATUS_FR_DESC,
u.USERNAME USER_NAME
--max(cast(ar.__Start_At.op_ts as timestamp))  Effective_Start_DT
From {bronze_database}.cpin_curam_users u
Left join {bronze_database}.cpin_curam_codetableitem ctie on U.statusCode
= ctie.code and ctie.TABLENAME ='RecordStatus' and ctie.LOCALEIDENTIFIER = 'en'
Left join {bronze_database}.cpin_curam_codetableitem ctif on U.statusCode
= ctif.code and ctif.TABLENAME ='RecordStatus' and ctif.LOCALEIDENTIFIER = 'fr' 