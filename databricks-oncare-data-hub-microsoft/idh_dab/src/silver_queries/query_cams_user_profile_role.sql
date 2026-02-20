Select  l.Description AGENCY_NAME,
a.Description APP_DESC,
r.APP_ID,
a.APP_TYPE_CODE,
a.ID APPLICATION_ID,
up.FIRST_NAME,
-- up.GUID,
up.GUID_TEXT,
ua.IS_ACTIVE_FLAG IS_ACTIVE_IND,
ua.IS_PRESENT_FLAG,
up.LAST_NAME,
l.LOCATION_CODE,
l.ID LOCATION_ID,
r.ROLE_CODE,
r.Description,
r.ID ROLE_ID,
ua.ID USER_ACCESS_ID,
up.USERNAME_TEXT USER_NAME_TEXT,
up.ID USER_PROFILE_ID,
up.WORK_EMAIL_TEXT
--cast(ua.__Start_At.op_ts as timestamp) Effective_Start_DT
From {bronze_database}.cpin_cams_user_access ua
join {bronze_database}.cpin_cams_user_profile up on ua.USER_ID  = up.ID
join {bronze_database}.cpin_cams_location l on ua.LOCATION_ID  = l.ID
join {bronze_database}.cpin_cams_role r on ua.ROLE_ID  = r.ID
join {bronze_database}.cpin_cams_application a on r.APP_ID  = a.ID
