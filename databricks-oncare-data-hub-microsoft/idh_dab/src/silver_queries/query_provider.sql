Select 
cast(p.providerconcernroleid as bigint) PROVIDER_CONCERN_ROLE_KEY,
P.AREASSVDINFOTXTID AREA_SSVD_INFO_TXT_ID,
P.CLIENTINFOTEXTID CLIENT_INFO_TEXT_ID,
cast(p.providerconcernroleid as bigint) CONCERN_ROLE_KEY,
pe.PROVIDERCONSENTFORCAPTURE CONSENT_FOR_CAPTURE_IND,
pe.PROVIDERCONSENTFORSEARCH CONSENT_FOR_SEARCH_IND,
p.currencyType CURRENCY_CODE,
pe.EMAILADDRESSID EMAIL_ADDRESS_ID,
P.ENDDATETIME END_DATETIME,
CAST(peq.PROVIDERENQUIRYID AS BIGINT) ENQUIRY_KEY,
p.enrollmentDateTime ENROLMENT_DATETIME,
p.recordStatus LATEST_STATUS_CODE,
coalesce(cti_en.DESCRIPTION,'')  LATEST_STATUS_EN_DESC,
coalesce(cti_fr.DESCRIPTION,'')  LATEST_STATUS_FR_DESC,
P.paymentFrequency PAYMENT_FREQUENCY_CODE,
P.methodOfPayment PAYMENT_METHOD_CODE,
p.physicalCapacity PHYSICAL_CAPACITY_VALUE,
p.preferredSEMethod PREF_SE_METHOD_CODE,
coalesce(cti_pm_en.DESCRIPTION,'')  PREF_SE_METHOD_EN_DESC,
coalesce(cti_pm_fr.DESCRIPTION,'')  PREF_SE_METHOD_FR_DESC,
pe.primaryProviderTypeID PRIM_PROVIDER_TYPE_ID,
p.providerConcernRoleID PROVIDER_CONCERN_ROLE_ID,
p.providerenquiryid PROVIDER_ENQUIRY_ID,
p.name PROVIDER_NAME,
p.nameUpper PROVIDER_UPPER_NAME,
coalesce(cai.ALTERNATEID,'')  REFERENCE_NUMBER
from {bronze_database}.cpin_curam_provider p
LEFT join {bronze_database}.cpin_curam_mcysproviderext pe on  p.providerconcernroleid = pe.providerconcernroleid
LEFT join {bronze_database}.cpin_curam_providerenquiry peq on p.providerenquiryid = peq.providerenquiryid 
LEFT join {bronze_database}.cpin_curam_concernrolealternateid cai on p.providerconcernroleid = cai.Concernroleid and cai.typecode='CA7'
LEFT join {bronze_database}.cpin_curam_codetableitem cti_en on p.recordstatus =cti_en.code and cti_en.TABLENAME = 'ProviderStatus' and cti_en.LOCALEIDENTIFIER = 'en'
LEFT join {bronze_database}.cpin_curam_codetableitem cti_fr on p.recordstatus =cti_fr.code and cti_fr.TABLENAME = 'ProviderStatus' and cti_fr.LOCALEIDENTIFIER = 'fr'
LEFT join {bronze_database}.cpin_curam_codetableitem cti_pm_en on p.preferredSEMethod =cti_pm_en.code and cti_pm_en.TABLENAME = 'PreferredSEMethod' and cti_pm_en.LOCALEIDENTIFIER = 'en'
LEFT join {bronze_database}.cpin_curam_codetableitem cti_pm_fr on p.preferredSEMethod =cti_pm_fr.code and cti_pm_fr.TABLENAME = 'PreferredSEMethod' and cti_pm_fr.LOCALEIDENTIFIER = 'fr'