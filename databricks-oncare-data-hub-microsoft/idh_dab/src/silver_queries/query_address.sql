SELECT  
    CAST(a.AddressId AS bigint) Address_Key,
    CAST(a.AddressId AS bigint) Address_Id,
    a.AddressLayoutType Address_Layout_Type,
    a.COUNTRYCODE Country_Code,
    a.MODIFIABLEIND Modifiable_Ind,
    ae.Address_1 Address1_Text,
    ae.Address_2 Address2_Text,
    ae.APT APT_Number,
    COALESCE(cti_a.EN_DESC,'') Coutry_EN_Name,
    COALESCE(cti_a.FR_DESC,'') Coutry_FR_Name,
    ae.City City_Name,
    ae.POBOXNO PO_Box_Number,
    ae.POSTCODE Postal_Code,
    CASE WHEN ae.POSTCODE IS NULL OR UPPER(ae.POSTCODE)='UNKNOWN' 
        THEN 'XOX' ELSE LEFT(ae.POSTCODE, 3) END POSTAL_FSA_CODE,
    CASE WHEN ae.POSTCODE IS NULL OR UPPER(ae.POSTCODE)='UNKNOWN' 
        THEN '0X0' ELSE LEFT(ae.POSTCODE, 3) END POSTAL_LDU_CODE,        
    ae.PROV Province_Name,
    '' State_Name,
    '' Zip_Code
FROM {bronze_database}.cpin_curam_address a
LEFT JOIN (
    SELECT * FROM (
        SELECT AddressId, ElementType, ElementValue
        FROM {bronze_database}.cpin_curam_addresselement
    )
    PIVOT (
        MAX(ElementValue) 
        FOR ElementType IN (
            'CITY' CITY, 'PROV' PROV, 'POSTCODE' POSTCODE, 'POBOXNO' POBOXNO,
            'COUNTRY' COUNTRY, 'ADD1' Address_1, 'ADD2' Address_2, 'APT' APT
        )
    )
) ae ON a.addressid = ae.addressid
LEFT JOIN {bronze_database}.cpin_curam_codetableitem_flat cti_a 
    ON a.countrycode = cti_a.code 
    AND cti_a.TABLENAME = 'AddressCountry'