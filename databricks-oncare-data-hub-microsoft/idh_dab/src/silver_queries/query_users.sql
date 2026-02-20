SELECT  hash('spark', u.USERNAME,2) USER_KEY,
coalesce(e.EmailAddress,'') BUSINESS_EMAIL_ADDRESS, 
coalesce(pn.PHONEAREACODE || ' - ' || pn.PHONENUMBER || ' Ext: ' || pn.PHONEEXTENSION, '') BUSINESS_PHONE, 
COALESCE(cn.PHONEAREACODE || ' - ' || cn.PHONENUMBER, '')  CELL_PHONE, 
u.creationDate CREATION_DATE, 
u.endDate END_DATE, 
u.firstname FIRST_NAME, 
u.fullName FULL_NAME, 

up.ID GUID, 
up.GUID_TEXT GUID_TEXT, 
u.surname LAST_NAME, 
mout.AGENCYCODE LOCATION_CODE, 
u.locationID LOCATION_ID, 
ool.ORGOBJECTLINKID ORG_OBJECT_LINK_ID, 
u.statusCode RECORD_STATUS_CODE, 
coalesce(cti_s.EN_DESC,'') RECORD_STATUS_EN_DESC, 
coalesce(cti_s.FR_DESC,'')  RECORD_STATUS_FR_DESC, 
u.roleName ROLE_NAME, 
u.sensitivity SENSITIVITY_IND, 
u.TITLE TITLE, 
u.upperFirstname UPPER_FIRST_NAME, 

u.upperSurname UPPER_LAST_NAME, 
u.upperRoleName UPPER_ROLE_NAME, 
u.upperUserName UPPER_USER_NAME, 
u.userName USER_NAME
--cast(u.__Start_At.op_ts as timestamp) Effective_Start_DT

From {bronze_database}.cpin_curam_users u
left join {bronze_database}.cpin_curam_phonenumber pn on u.BUSINESSPHONEID   = pn.PHONENUMBERID   
left join {bronze_database}.cpin_curam_phonenumber cn on u.MOBILEPHONEID   = cn.PHONENUMBERID   
left join {bronze_database}.cpin_curam_emailaddress e on u.BUSINESSEMAILID = e.EMAILADDRESSID 
left join {bronze_database}.cpin_cams_user_profile up on u.UPPERUSERNAME  = upper(up.USERNAME_TEXT)
join {bronze_database}.cpin_curam_location l on u.LOCATIONID  = l.LOCATIONID
-- join {bronze_database}.cpin_curam_organisationunit ou on l.ORGANISATIONID  = ou.ORGANISATIONUNITID
left join {bronze_database}.cpin_curam_mcysorganisationunitext mout on l.ORGANISATIONID  = mout.ORGANISATIONUNITID
left join {bronze_database}.cpin_curam_orgobjectlink ool on u.UPPERUSERNAME   = upper(ool.USERNAME)
left join {bronze_database}.cpin_curam_codetableitem_flat cti_s on u.statusCode = cti_s.code and cti_s.TABLENAME ='RecordStatus'
