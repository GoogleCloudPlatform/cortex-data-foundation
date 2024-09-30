# -- Copyright 2024 Google LLC
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --      https://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.

/* Google Analytics 4 Events Details */

SELECT
  property_id,
  PARSE_DATE('%Y%m%d', occurrence_date) AS occurrence_date,
  pseudo_user_id,
  stream_id,
  PARSE_DATE('%Y%m%d', last_updated_date) AS last_updated_date,
  * EXCEPT (
    property_id,
    occurrence_date,
    pseudo_user_id,
    stream_id,
    last_updated_date
  )
FROM
  (
    {% for property in marketing_ga4_datasets_cdc %}
      SELECT
        {{ property.property_id }} AS property_id, *
      FROM `{{ project_id_src }}.{{ property.name }}.pseudonymous_users_*`
      {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
  )
