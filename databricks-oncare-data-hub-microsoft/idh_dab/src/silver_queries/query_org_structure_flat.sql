Select oupl.ORGUNITPARENTLINKID ORG_UNIT_PARENT_LINK_ID, 
oupl.ORGANISATIONUNITID ORGANISATION_UNIT_ID, 
oupl.PARENTORGANISATIONUNITID PARENT_ORG_UNIT_ID, 
opl.POSITIONID POSITION_ID, 
coalesce(os.NAME,'') STRUCTURE_NAME, 
ou.NAME TEAM_NAME
From {bronze_database}.cpin_curam_orgunitparentlink oupl 
left join {bronze_database}.cpin_curam_organisationunit ou on oupl.ORGANISATIONUNITID = ou.ORGANISATIONUNITID 
left join {bronze_database}.cpin_curam_orgunitpositionlink opl on ou.ORGANISATIONUNITID = opl.ORGANISATIONUNITID 
left join {bronze_database}.cpin_curam_organisationstructure os on oupl.ORGANISATIONSTRUCTUREID = os.ORGANISATIONSTRUCTUREID
