#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

## Following currency conversion functions are deprecated, and will be removed in the next release.
## Use currency_conversion table instead.

CREATE TABLE IF NOT EXISTS `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion`
(
  mandt STRING,
  kurst STRING,
  fcurr STRING,
  tcurr STRING,
  ukurs NUMERIC,
  start_date DATE,
  end_date DATE,
  conv_date DATE
)
PARTITION BY conv_date;

{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Currency_Conversion`(
  ip_mandt STRING, ip_kurst STRING, ip_fcurr STRING, ip_tcurr STRING, ip_date DATE, ip_amount NUMERIC
) AS
{% include './ecc/currency_conversion.sql' -%}
;
{% endif -%}


{% if sql_flavour == 's4' or sql_flavour == 'union' -%}

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Currency_Conversion`(
  ip_mandt STRING, ip_kurst STRING, ip_fcurr STRING, ip_tcurr STRING, ip_date DATE, ip_amount NUMERIC
) AS
{% include './s4/currency_conversion.sql' -%}
;
{% endif -%}
