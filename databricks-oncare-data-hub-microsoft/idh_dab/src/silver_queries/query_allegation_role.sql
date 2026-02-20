Select  ar.CASEPARTICIPANTROLEID as CASE_PARTICIPANT_ROLE_ID,
        cr.CONCERNROLEID as CONCERN_ROLE_ID, 
        coalesce(ctie.DESCRIPTION,'') as ROLE_TYPE_EN_DESC, 
        max(coalesce(ctif.DESCRIPTION,'')) as ROLE_TYPE_FR_DESC
        --max(cast(ar.__Start_At.op_ts as timestamp))  Effective_Start_DT
From {bronze_database}.cpin_curam_allegationrole ar
Join {bronze_database}.cpin_curam_caseparticipantrole cpr on ar.CASEPARTICIPANTROLEID = cpr.CASEPARTICIPANTROLEID 
Join {bronze_database}.cpin_curam_concernrole cr ON cr.concernroleid = cpr.participantroleid
left join {bronze_database}.cpin_curam_codetableitem ctie on ar.ROLETYPE = ctie.code and ctie.TABLENAME ='AllegationRoleType' and ctie.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif on ar.ROLETYPE = ctif.code and ctif.TABLENAME ='AllegationRoleType' and ctif.LOCALEIDENTIFIER = 'fr'
Group By cr.CONCERNROLEID, ctie.DESCRIPTION, ar.CASEPARTICIPANTROLEID 