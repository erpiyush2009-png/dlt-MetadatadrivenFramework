SELECT  cast(pp.PROVIDERPARTYID as bigint) PROVIDER_PARTY_KEY,
coalesce(cti_c.EN_DESC,'') CATEGORY_EN_DESC,
coalesce(cti_c.FR_DESC,'') CATEGORY_FR_DESC,
mpp.DATEOFBIRTH DOB_DATE,
coalesce(cti_p.EN_DESC, '') POSITION_EN_DESC,
coalesce(cti_p.FR_DESC, '') POSITION_FR_DESC,
cr.CONCERNROLENAME PROVIDER_PARTY_NAME,
coalesce(cti_s.EN_DESC, '') RECORD_STATUS_EN_DESC,
coalesce(cti_s.FR_DESC, '') RECORD_STATUS_FR_DESC,
cr.CONCERNROLEID REFERENCE_NUMBER,
coalesce(cti_m.EN_DESC, '') ROLE_EN_DESC,
coalesce(cti_m.FR_DESC, '') ROLE_FR_DESC
From {bronze_database}.cpin_curam_providerparty pp

left join {bronze_database}.cpin_curam_mcysproviderpartyext mpp on pp.PROVIDERPARTYID = mpp.PROVIDERPARTYID
left join {bronze_database}.cpin_curam_concernrole cr on pp.PARTYCONCERNROLEID = cr.CONCERNROLEID 

left join {bronze_database}.cpin_curam_codetableitem_flat cti_c on pp.CATEGORY = cti_c.code and cti_c.TABLENAME ='ProviderPartyCategory'
left join {bronze_database}.cpin_curam_codetableitem_flat cti_p on pp.POSITION = cti_p.code and cti_p.TABLENAME ='ProviderMemberPosition'
left join {bronze_database}.cpin_curam_codetableitem_flat cti_s on pp.RECORDSTATUS = cti_s.code and cti_s.TABLENAME ='RecordStatus'
left join {bronze_database}.cpin_curam_codetableitem_flat cti_m on pp.TYPE = cti_m.code and cti_m.TABLENAME ='ProviderMemberRole'
