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

## Following sample script uses the deprecated "csks_hier" tables.
## It will be updated to use the new "cost_centers" tables in the next release.

{% if sql_flavour == 'ecc' -%}
({% include './ecc/CostCenterAmountsHierarchy_SAMPLE.sql' -%})
{% endif -%}

{% if sql_flavour == 's4' -%}
({% include './s4/CostCenterAmountsHierarchy_SAMPLE.sql' -%})
{% endif -%}

