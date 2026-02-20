Select 	clc.CONTACTLOGCONCERNID,
cti_s.EN_DESC CL_CONCERNING_STATUS_EN_DESC, 
cti_s.FR_DESC CL_CONCERNING_STATUS_FR_DESC, 
cr.CONCERNROLETYPE  CONCERN_ROLE_TYPE_CODE, 
cti_rt.EN_DESC CONCERN_ROLE_TYPE_EN_DESC, 
cti_rt.FR_DESC CONCERN_ROLE_TYPE_FR_DESC
From   {bronze_database}.cpin_curam_contactlogconcern clc
left join  {bronze_database}.cpin_curam_concernrole cr on clc.CONCERNROLEID  = cr.CONCERNROLEID
left join  {bronze_database}.cpin_curam_codetableitem_flat cti_s on clc.RECORDSTATUS = cti_s.code and cti_s.TABLENAME ='RecordStatus' 
left join  {bronze_database}.cpin_curam_codetableitem_flat cti_rt on cr.CONCERNROLETYPE = cti_rt.code and cti_rt.TABLENAME ='CaseParticipantRoleType'
