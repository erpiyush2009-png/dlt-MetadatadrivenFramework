Select  
an.ALTERNATENAMEID ALTERNATE_NAME_ID,
an.COMMENTS,
an.CONCERNROLEID CONCERN_ROLE_ID,
an.FIRSTFORENAME FIRST_NAME,
man.FORENAMEPHONETICENC FIRSTNAME_PHONETIC_ENC,
an.FULLNAME FULL_NAME,
an.INITIALS,
an.SURNAME LAST_NAME,
an.NAMESTATUS NAME_STATUS_CODE,
an.NAMESUFFIX NAME_SUFFIX_CODE,
an.NAMETYPE NAME_TYPE_CODE,
Coalesce(ctie_t.Description,'') NAME_TYPE_EN_DESC,
Coalesce(ctif_t.Description,'') NAME_TYPE_FR_DESC,       		
an.OTHERFORENAME OTHER_FIRST_NAME,
an.PHONETICENCODING PHONETIC_ENCODING_CODE,
Coalesce(ctie_s.Description,'') RECORD_STATUS_EN_DESC,
Coalesce(ctif_s.Description,'') RECORD_STATUS_FR_DESC,  
man.SINGLENAMEIND SINGLE_NAME_IND,
an.TITLE TITLE_TEXT,
an.UPPERFIRSTFORENAME UPPER_FIRST_NAME,
man.UPPERFULLNAME UPPER_FULL_NAME,
an.UPPERSURNAME UPPER_LAST_NAME
From {bronze_database}.cpin_curam_alternatename an
Left Join {bronze_database}.cpin_curam_mcysalternatenameext man on an.ALTERNATENAMEID = man.ALTERNATENAMEID
Left join {bronze_database}.cpin_curam_codetableitem ctie_s on an.NAMESTATUS = ctie_s.code and ctie_s.TABLENAME ='RecordStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
Left join {bronze_database}.cpin_curam_codetableitem ctif_s on an.NAMESTATUS = ctif_s.code and ctif_s.TABLENAME ='RecordStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
Left join {bronze_database}.cpin_curam_codetableitem ctie_t on an.NAMETYPE = ctie_t.code and ctie_t.TABLENAME ='AlternateNameType' and ctie_t.LOCALEIDENTIFIER = 'en'
Left join {bronze_database}.cpin_curam_codetableitem ctif_t on an.NAMETYPE = ctif_t.code and ctif_t.TABLENAME ='AlternateNameType' and ctif_t.LOCALEIDENTIFIER = 'fr'
