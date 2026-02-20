Select 	cast(po.PROVIDEROFFERINGID as bigint) PROVIDER_OFFERING_KEY, 
po.clientFeeInfTextID CLIENT_FEE_INFO_TEXT_ID, 
po.comments COMMENTS_TEXT, 
po.DENIALREASON DENIAL_REASON_CODE, 
cti_dr.EN_DESC DENIAL_REASON_EN_DESC, 
cti_dr.FR_DESC DENIAL_REASON_FR_DESC, 
po.docsReqdInfTextID DOCS_REQD_INFO_TEXT_ID, 
po.clientFeeInfTextID ELIGIBILITY_INFO_TEXT_ID, 
po.endDate END_DATE, 
po.endReason END_REASON_CODE, 
cti_er.EN_DESC END_REASON_EN_DESC, 
cti_er.FR_DESC END_REASON_FR_DESC, 
po.intkProcInfoTextID INTK_PROC_INFO_TEXT_ID, 
po.providerConcernRoleID PROVIDER_CONCERN_ROLE_ID, 
po.providerOfferingID PROVIDER_OFFERING_ID, 
po.recordStatus RECORD_STATUS_CODE, 
cti_s.EN_DESC RECORD_STATUS_EN_DESC, 
cti_s.FR_DESC RECORD_STATUS_FR_DESC, 
po.serviceOfferingID SERVICE_OFFERING_ID, 
po.startDate START_DATE
--cast(po.__Start_At.op_ts as timestamp) Effective_Start_DT
From {bronze_database}.cpin_curam_provideroffering po
left join {bronze_database}.cpin_curam_codetableitem_flat cti_dr on po.DENIALREASON = cti_dr.code and cti_dr.TABLENAME ='ProvOfferDenialReason' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_er on po.endReason = cti_er.code and cti_er.TABLENAME ='ProviderOfferingEndReason' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_s on po.recordStatus = cti_s.code and cti_s.TABLENAME ='ProviderOfferingEndReason'
