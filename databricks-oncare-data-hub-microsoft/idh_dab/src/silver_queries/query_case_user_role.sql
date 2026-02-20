Select  
hash('spark', u.USERNAME,2) USER_KEY,
obl.USERNAME USER_NAME,
ctie_urt.Description CU_ROLE_TYPE_EN_DESC,
max(ctif_urt.Description) CU_ROLE_TYPE_FR_DESC,
max(coalesce(ctie_rr.Description, '')) REASSIGN_REASON_EN_DESC,
max(coalesce(ctif_rr.Description, '')) REASSIGN_REASON_FR_DESC,
max(ctie_s.Description) RECORD_STATUS_EN_DESC,
max(ctif_s.Description) RECORD_STATUS_FR_DESC
From {bronze_database}.cpin_curam_caseuserrole ur
join {bronze_database}.cpin_curam_orgobjectlink obl on ur.ORGOBJECTLINKID = obl.ORGOBJECTLINKID and obl.ORGOBJECTTYPE='RL9'
join {bronze_database}.cpin_curam_users u on u.USERNAME = obl.USERNAME
left join {bronze_database}.cpin_curam_codetableitem ctie_urt on ur.TYPECODE  = ctie_urt.code and ctie_urt.TABLENAME ='CaseUserRoleType' and ctie_urt.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_urt on ur.TYPECODE = ctif_urt.code and ctif_urt.TABLENAME ='CaseUserRoleType' and ctif_urt.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_rr on ur.REASONCODE = ctie_rr.code and ctie_rr.TABLENAME ='CaseReassignReason' and ctie_rr.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_rr on ur.REASONCODE = ctif_rr.code and ctif_rr.TABLENAME ='CaseReassignReason' and ctif_rr.LOCALEIDENTIFIER = 'fr'
left join {bronze_database}.cpin_curam_codetableitem ctie_s on ur.RECORDSTATUS = ctie_s.code and ctie_s.TABLENAME ='RecordStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
left join {bronze_database}.cpin_curam_codetableitem ctif_s on ur.RECORDSTATUS = ctif_s.code and ctif_s.TABLENAME ='RecordStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
Group By u.USERNAME, obl.USERNAME, ctie_urt.Description
