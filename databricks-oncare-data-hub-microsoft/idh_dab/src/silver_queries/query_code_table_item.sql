Select  ctie.TABLENAME TABLE_NAME,
ctie.CODE ITEM_CODE,
ctie.PARENTCODE PARENT_CODE,
ctie.ISENABLED IS_ENABLED,
ctie.COMMENTS COMMENT_TEXT,
ctie.SORTORDER SORT_ORDER,
ctie.ANNOTATION ANNOTATION_EN_DESC,
COALESCE(ctif.ANNOTATION,'') ANNOTATION_FR_DESC,
ctie.DESCRIPTION EN_DESCRIPTION,
COALESCE(ctif.DESCRIPTION,'') FR_DESCRIPTION
From {bronze_database}.cpin_curam_codetableitem ctie
left join {bronze_database}.cpin_curam_codetableitem ctif on ctie.TABLENAME  = ctif.TABLENAME and ctie.CODE  = ctif.CODE and ctif.LOCALEIDENTIFIER  = 'fr'
Where ctie.LOCALEIDENTIFIER = 'en'  