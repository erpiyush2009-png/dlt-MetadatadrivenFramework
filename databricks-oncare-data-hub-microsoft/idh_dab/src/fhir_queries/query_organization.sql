select  'prov' ORGANZATION_TYPE, 
		cast(p.PROVIDER_CONCERN_ROLE_ID as bigint) as ORGANIZATION_ID, 
		
		p.PROVIDER_NAME ORGANIZATION_NAME, 
		p.REFERENCE_NUMBER, 
		p.LATEST_STATUS_EN_DESC,
		-- COLLECT_LIST(NAMED_STRUCT('line', coalesce(a.APT_Number || ' ', '') || coalesce(a.Address1_Text, '') || coalesce(', ' || a.Address2_Text, ''),
		--								'city', a.City_Name, 'state', a.Province_Name, 'country', a.Coutry_EN_Name, 'postalCode', a.Postal_Code, 'use', 'work', 'type', 'both')) AS addresses
		coalesce(a.APT_Number || ' ', '')  || coalesce(a.Address1_Text, '') || coalesce(', ' || a.Address2_Text, '') AS address_line,
		a.City_Name, 
		a.Province_Name, 
		a.Coutry_EN_Name, 
		a.Postal_Code, 
		'work' AS address_use, 
		'both' address_type
		
FROM {silver_schema}.provider p
left join {silver_schema}.concern_role cr on p.CONCERN_ROLE_KEY = cr.CONCERN_ROLE_KEY
left join {silver_schema}.address a on cr.PRIMARY_ADDRESS_ID = a.Address_Id

UNION ALL

select  'govt' ORGANZATION_TYPE, 
		cast(u.ORGANISATION_UNIT_ID as bigint) as ORGANIZATION_ID, 
		
		u.ORGANISATION_NAME ORGANIZATION_NAME, 
		u.ORGANISATION_UNIT_ID REFERENCE_NUMBER, 
		'' LATEST_STATUS_EN_DESC,
		-- COLLECT_LIST(NAMED_STRUCT('line', coalesce(a.APT_Number || ' ', '') || coalesce(a.Address1_Text, '') || coalesce(', ' || a.Address2_Text, ''),
		--							'city', a.City_Name, 'state', a.Province_Name, 'country', a.Coutry_EN_Name, 'postalCode', a.Postal_Code, 'use', 'work', 'type', 'both')) AS addresses
		coalesce(a.APT_Number || ' ', '')  || coalesce(a.Address1_Text, '') || coalesce(', ' || a.Address2_Text, '') AS address_line,
		a.City_Name, 
		a.Province_Name, 
		a.Coutry_EN_Name, 
		a.Postal_Code, 
		'work' AS address_use, 
		'both' address_type	

FROM {silver_schema}.org_unit u
left join {silver_schema}.address a on u.Address_Id = a.Address_Id
where u.ORGANISATION_NAME is not null
