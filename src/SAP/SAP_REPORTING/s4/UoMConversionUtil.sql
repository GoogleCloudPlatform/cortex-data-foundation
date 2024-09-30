(WITH t6_out AS (
  SELECT
    t6_out.mandt, t6_out.msehi, t6_out.dimid, t6_out.nennr, t6_out.zaehl, t6_out.addko, cast(t6_out.exp10 AS INT64) AS exp10
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t006` AS t6_out
  LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t006` AS t6_in
    ON T6_out.mandt = t6_in.mandt
  WHERE t6_in.mandt = '{{ mandt_s4 }}'

),

t6_in AS (
  SELECT
    t6_in.mandt, t6_in.msehi, t6_in.dimid, t6_in.nennr, t6_in.zaehl, t6_in.addko, cast(t6_in.exp10 AS INT64) AS exp10,
    if(t6_in.zaehl != 0, ( t6_in.nennr / t6_in.zaehl ), 0 ) AS to_conv
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t006` AS t6_in
  WHERE t6_in.mandt = '{{ mandt_s4 }}'

)

SELECT
  t6_out.msehi AS unit_to, t6_in.msehi AS unit_from, t6_in.mandt,
  #t6_out.msehi as unit_out, t6_out.dimid, t6_out.nennr, t6_out.zaehl,
  # t6_in.nennr as to_nennr, t6_in.zaehl as to_zaehl, t6_in.to_conv,
  #(t6_out.exp10 - t6_in.exp10) as exp_pos,
  #(val_in * ( t6_out.zaehl * t6_in.nennr ) ) as num_neg,
  #(t6_in.zaehl * t6_out.nennr ) * if(t6_out.exp10 > 0  and t6_in.exp10 > 0, (10 ^ ( - t6_out.exp10 - t6_in.exp10 ) ), 1 ) as denom_neg,
  #( val_in * ( (t6_in.zaehl * t6_out.nennr ) * if(t6_out.exp10 > 0  and t6_in.exp10 > 0, (10 ^ (  t6_out.exp10 - t6_in.exp10 ) ), 1 ) ) )  as num_pos,
  #( t6_out.zaehl * t6_in.nennr ) as denom_pos,
  if(t6_in.dimid != t6_out.dimid, 'ERROR', t6_in.dimid) AS dimension,
  if( t6_out.exp10 - t6_in.exp10 < 0,

    ( t6_out.zaehl * t6_in.nennr )--Numerator

    / (t6_in.zaehl * t6_out.nennr ) * if(t6_out.exp10 > 0 AND t6_in.exp10 > 0, (10 ^ ( - t6_out.exp10 - t6_in.exp10 ) ), 1 ) -- Denominator

    + ( (t6_out.addko - t6_in.addko ) * t6_in.to_conv ) * if(t6_in.exp10 != 0, ( 10 ^ ( -t6_in.exp10 ) ), 1 ),   -- Addition
     --if t6_out.exp10 - t6_in.exp10 > 0 POS

    ( ( (t6_in.zaehl * t6_out.nennr ) * if(t6_out.exp10 > 0 AND t6_in.exp10 > 0, (10 ^ ( t6_out.exp10 - t6_in.exp10 ) ), 1 ) ) -- Numerator

      / ( t6_out.zaehl * t6_in.nennr ) ) -- Denominator

    + if(t6_out.addko - t6_in.addko != 0,

      ( (t6_out.addko - t6_in.addko ) * t6_in.to_conv ) * if(t6_in.exp10 != 0, ( 10 ^ ( -t6_in.exp10 ) ), 1 ),

      0 )

  ) AS val_out

--t6_in.dimid, t6_in.nennr, t6_in.zaehl, t6_in.addko
FROM t6_out INNER JOIN t6_in ON t6_out.mandt = t6_in.mandt
WHERE t6_in.mandt = '{{ mandt_s4 }}'
)
