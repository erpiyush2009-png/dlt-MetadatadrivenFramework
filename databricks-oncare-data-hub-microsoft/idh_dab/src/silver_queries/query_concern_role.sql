Select  cr.CONCERNID CONCERN_ID,
cr.CONCERNROLEID CONCERN_ROLE_ID,
cast(cr.CONCERNROLEID as bigint) CONCERN_ROLE_KEY,
cr.CONCERNROLENAME CONCERN_ROLE_NAME,
cr.CONCERNROLETYPE CONCERN_ROLE_TYPE_CODE,
coalesce(ctie_rt.DESCRIPTION,'') CONCERN_ROLE_TYPE_EN_DESC,  
coalesce(ctif_rt.DESCRIPTION,'') CONCERN_ROLE_TYPE_FR_DESC,
cr.COMMENTS CR_COMMENTS_TEXT,
cr.PRIMARYALTERNATEID CR_PRIM_ALTERNATE_ID,
cr.CREATIONDATE CREATION_DATE,
cr.ENDDATE END_DATE,
cr.PREFCOMMFROMDATE PREF_COMM_FROM_DATE,
cr.PREFCOMMMETHOD PREF_COMM_METHOD_CODE,
coalesce(ctie_pcm.DESCRIPTION,'') PREF_COMM_METHOD_EN_DESC,  
coalesce(ctif_pcm.DESCRIPTION,'') PREF_COMM_METHOD_FR_DESC,
cr.PREFCOMMTODATE PREF_COMM_TO_DATE,
cr.PREFERREDLANGUAGE PREF_LANGUAGE_CODE,
coalesce(ctie_pl.DESCRIPTION,'') PREF_LANGUAGE_EN_DESC,  
coalesce(ctif_pl.DESCRIPTION,'') PREF_LANGUAGE_FR_DESC,
cr.PREFPUBLICOFFICEID PREF_PUBLIC_OFFICE_ID,
cr.PRIMARYEMAILADDRESSID PRIM_EMAIL_ADDRESS_ID,
coalesce(ea.EMAILADDRESS,'') PRIM_EMAIL_ADDRESS_TEXT,
cr.PRIMARYPHONENUMBERID PRIM_PHONE_NUMBER_ID,
cr.PRIMARYADDRESSID PRIMARY_ADDRESS_ID,
cr.REGUSERNAME REG_USER_NAME,
cr.REGISTRATIONDATE REGISTRATION_DATE,
cr.SENSITIVITY SENSITIVITY_IND,
cr.STARTDATE START_DATE,
cr.STATUSCODE STATUS_CODE,
coalesce(ctie_s.DESCRIPTION,'') STATUS_EN_DESC,
coalesce(ctif_s.DESCRIPTION,'') STATUS_FR_DESC
From {bronze_database}.cpin_curam_concernrole cr
left join {bronze_database}.cpin_curam_emailaddress ea on cr.PRIMARYEMAILADDRESSID  = ea.EMAILADDRESSID
left join {bronze_database}.cpin_curam_codetableitem ctie_rt on cr.CONCERNROLETYPE  = ctie_rt.code and ctie_rt.TABLENAME ='CaseParticipantRoleType' and ctie_rt.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_rt on cr.CONCERNROLETYPE = ctif_rt.code and ctif_rt.TABLENAME ='CaseParticipantRoleType' and ctif_rt.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_pcm on cr.PREFCOMMMETHOD = ctie_pcm.code and ctie_pcm.TABLENAME ='CommunicationMethod' and ctie_pcm.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_pcm on cr.PREFCOMMMETHOD = ctif_pcm.code and ctif_pcm.TABLENAME ='CommunicationMethod' and ctif_pcm.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_pl on cr.PREFERREDLANGUAGE = ctie_pl.code and ctie_pl.TABLENAME ='PreferredLanguage' and ctie_pl.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_pl on cr.PREFERREDLANGUAGE = ctif_pl.code and ctif_pl.TABLENAME ='PreferredLanguage' and ctif_pl.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_s on cr.STATUSCODE = ctie_s.code and ctie_s.TABLENAME ='RecordStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_s on cr.STATUSCODE = ctif_s.code and ctif_s.TABLENAME ='RecordStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
