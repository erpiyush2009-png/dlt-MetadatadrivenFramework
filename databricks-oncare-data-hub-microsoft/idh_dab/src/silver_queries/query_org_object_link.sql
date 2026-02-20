SELECT  ol.ORGOBJECTLINKID ORG_OBJECT_LINK_ID, 
ol.ORGOBJECTREFERENCE ORG_OBJECT_REFERENCE_ID, 
ol.ORGOBJECTTYPE ORG_OBJECT_TYPE_CODE, 
coalesce(cti_ot.EN_DESC,'') ORG_OBJECT_TYPE_EN_DESC, 
coalesce(cti_ot.FR_DESC,'') ORG_OBJECT_TYPE_FR_DESC
From {bronze_database}.cpin_curam_orgobjectlink ol
left join {bronze_database}.cpin_curam_codetableitem_flat cti_ot on ol.ORGOBJECTTYPE = cti_ot.code and cti_ot.TABLENAME ='OrgObjectType' 
