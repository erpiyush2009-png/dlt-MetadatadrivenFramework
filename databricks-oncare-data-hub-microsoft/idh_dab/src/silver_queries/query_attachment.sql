Select  
cast(a.ATTACHMENTID as bigint) ATTACHMENT_KEY,
a.ATTACHMENTNAME ATTACHMENT_FILE_NAME,
a.ATTACHMENTID ATTACHMENT_ID,
a.STATUSCODE ATTACHMENT_STATUS_CODE,
Coalesce(ctie_s.DESCRIPTION,'') ATTACHMENT_STATUS_EN_DESC,
Coalesce(ctif_s.DESCRIPTION,'') ATTACHMENT_STATUS_FR_DESC,
a.DOCUMENTTYPE DOCUMENT_TYPE_CODE,
Coalesce(ctie_dt.DESCRIPTION,'') DOCUMENT_TYPE_EN_DESC,
Coalesce(ctif_dt.DESCRIPTION,'') DOCUMENT_TYPE_FR_DESC,
a.FILELOCATION FILE_LOCATION_DESC,
a.FILEREFERENCE FILE_REFERENCE_DESC,
a.RECEIPTDATE RECEIPT_DATE
-- ctie_rat.DESCRIPTION RELATED_ATTACH_TYPE_EN_DESC,   -- NOT able to find join
-- ctif_rat.DESCRIPTION RELATED_ATTACH_TYPE_FR_DESC,   -- NOT able to find join
--cast(a.__Start_At.op_ts as timestamp) Effective_Start_DT
From {bronze_database}.cpin_curam_attachment a
Left join {bronze_database}.cpin_curam_codetableitem ctie_s on a.ATTACHMENTSTATUS  = ctie_s.code and ctie_s.TABLENAME ='AttachmentStatus' and ctie_s.LOCALEIDENTIFIER = 'en'
Left join {bronze_database}.cpin_curam_codetableitem ctif_s on a.ATTACHMENTSTATUS = ctif_s.code and ctif_s.TABLENAME ='AttachmentStatus' and ctif_s.LOCALEIDENTIFIER = 'fr'
Left join {bronze_database}.cpin_curam_codetableitem ctie_dt on a.DOCUMENTTYPE = ctie_dt.code and ctie_dt.TABLENAME ='DocumentType' and ctie_dt.LOCALEIDENTIFIER = 'en'
Left join {bronze_database}.cpin_curam_codetableitem ctif_dt on a.DOCUMENTTYPE = ctif_dt.code and ctif_dt.TABLENAME ='DocumentType' and ctif_dt.LOCALEIDENTIFIER = 'fr'