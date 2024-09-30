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

## Following sample script uses the deprecated "cepc_hier" table.
## It will be updated to use the new "profit_centers" table in the next release.

{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
{% include './ecc/ProfitCenterAmountsHierarchy_SAMPLE.sql' -%}
{% endif -%}

{% if sql_flavour == 'union' -%}
UNION ALL
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
{% include './s4/ProfitCenterAmountsHierarchy_SAMPLE.sql' -%}
{% endif -%}
