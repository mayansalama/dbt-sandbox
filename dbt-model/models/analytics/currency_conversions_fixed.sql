
SELECT a.currency_conversion_id
      ,a.currency_id
      ,b.currency
      ,a.day_value
      ,CASE WHEN b.currency = 'AUD' THEN 1 ELSE a.to_aud END as to_aud
FROM {{ source(var('base.currency_conversion')) }} a
LEFT JOIN {{ source(var('base.currency')) }} b on a.currency_id = b.currency_id
