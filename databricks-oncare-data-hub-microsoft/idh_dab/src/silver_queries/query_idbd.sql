Select 	m.CONCERNROLEID,
coalesce(cti_ai.EN_DESC, '') ABORIG_IDENTITY_EN_DESC, 
coalesce(cti_ai.FR_DESC, '') ABORIG_IDENTITY_FR_DESC, 
coalesce(cti_ind.EN_DESC, '') ABORIG_TYPE_EN_DESC, 
coalesce(cti_ind.FR_DESC, '') ABORIG_TYPE_FR_DESC, 
coalesce(cti_bs.EN_DESC, '') BIRTH_SEX_EN_DESC, 
coalesce(cti_bs.FR_DESC, '') BIRTH_SEX_FR_DESC, 
coalesce(cti_byi.EN_DESC, '') BIRTH_YEAR_TYPE_EN_DESC, 
coalesce(cti_byi.FR_DESC, '') BIRTH_YEAR_TYPE_FR_DESC, 
coalesce(cti_bcp.EN_DESC, '') BORN_CANADA_PROV_EN_DESC, 
coalesce(cti_bcp.FR_DESC, '') BORN_CANADA_PROVI_FR_DESC, 
coalesce(cti_c.EN_DESC, '') BORN_COUNTRY_EN_DESC, 
coalesce(cti_c.FR_DESC, '') BORN_COUNTRY_FR_DESC, 
coalesce(cti_bci.EN_DESC, '') BORN_IN_CANADA_TYPE_EN_DESC, 
coalesce(cti_bci.FR_DESC, '') BORN_IN_CANADA_TYPE_FR_DESC, 
coalesce(cti_cr.EN_DESC, '') CHANGE_REASON_EN_DESC, 
coalesce(cti_cr.FR_DESC, '') CHANGE_REASON_FR_DESC, 
coalesce(cti_ci.EN_DESC, '') CONSENT_EN_DESC, 
coalesce(cti_ci.FR_DESC, '') CONSENT_FR_DESC, 
coalesce(cti_di.EN_DESC, '') DISABILITY_EN_DESC, 
coalesce(cti_di.FR_DESC, '') DISABILITY_FR_DESC, 
coalesce(cti_fs.EN_DESC, '') FAMILY_STATUS_EN_DESC, 
coalesce(cti_fs.FR_DESC, '') FAMILY_STATUS_FR_DESC, 
coalesce(cti_ms.EN_DESC, '') MARITAL_STATUS_EN_DESC, 
coalesce(cti_ms.FR_DESC, '') MARITAL_STATUS_FR_DESC, 
coalesce(cti_pci.EN_DESC, '') POSTAL_CODE_TYPE_EN_DESC, 
coalesce(cti_pci.FR_DESC, '') POSTAL_CODE_TYPE_FR_DESC
From {bronze_database}.cpin_curam_mcysidbd m
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ai on m.ABORIGINALIDENTITY = cti_ai.code and cti_ai.TABLENAME ='MCYSIDBDAboriginalIdnty' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ind on m.ABORIGINALIND = cti_ind.code and cti_ind.TABLENAME ='MCYSIDBDAboriginalInd' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_bs on m.BIRTHSEX = cti_bs.code and cti_bs.TABLENAME ='MCYSIDBDBirthSex' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_byi on m.BIRTHYEARIND = cti_byi.code and cti_byi.TABLENAME ='MCYSIDBDBirthYearInd' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_bcp on m.BORNCANADAPROVINCE = cti_bcp.code and cti_bcp.TABLENAME ='MCYSIDBDBornCanProvince' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_c on m.BORNCOUNTRY = cti_c.code and cti_c.TABLENAME ='MCYSIDBDCountry' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_bci on m.BORNINCANADAIND = cti_bci.code and cti_bci.TABLENAME ='MCYSIDBDBornCanadaInd' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_cr on m.CHANGEREASON = cti_cr.code and cti_cr.TABLENAME ='MCYSIDBDChangeReason' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ci on m.CONSENTIND = cti_ci.code and cti_ci.TABLENAME ='MCYSIDBDConsentInd' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_di on m.DISABILITYIND = cti_di.code and cti_di.TABLENAME ='MCYSIDBDDisabilityInd' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_fs on m.FAMILYSTATUS = cti_fs.code and cti_fs.TABLENAME ='MCYSIDBDFamilyStatus' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ms on m.MARITALSTATUS = cti_ms.code and cti_ms.TABLENAME ='MCYSIDBDMaritalStatus' 
left join {bronze_database}.cpin_curam_codetableitem_flat cti_pci on m.POSTALCODEIND = cti_pci.code and cti_pci.TABLENAME ='MCYSIDBDPostalCodeInd' 
