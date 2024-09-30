-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterMapping`() --noqa:LT01
BEGIN
  --This procedure generates table having profit center mapped to profit center hierarchy nodes.
  DECLARE max_iterations INT64 DEFAULT NULL;

  SET max_iterations = (
    SELECT MAX(level) FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
  );

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers` (
    mandt STRING,
    setclass STRING,
    subclass STRING,
    hiername STRING,
    parent STRING,
    node STRING,
    profitcenter STRING,
    level INT64,
    isleafnode BOOL) --noqa:LT02
  AS (
    WITH RECURSIVE
      ProfitHierarchy AS (
        --Insert leaf nodes with profit center mapping.
        SELECT
          mandt,
          setclass,
          subclass,
          hiername,
          parent,
          node,
          node AS profitcenter,
          level,
          isleafnode,
          0 AS current_iteration
        FROM
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
        WHERE
          isleafnode
          AND level = 0

        UNION ALL

        --Insert profit center mapping by parent node combination for each level.
        SELECT
          ProfitHierarchy.mandt,
          ProfitHierarchy.setclass,
          ProfitHierarchy.subclass,
          ProfitHierarchy.hiername,
          ProfitCenterFlattened.parent,
          ProfitCenterFlattened.node,
          ProfitHierarchy.profitcenter,
          ProfitCenterFlattened.level,
          ProfitCenterFlattened.isleafnode,
          ProfitHierarchy.current_iteration + 1 AS current_iteration
        FROM
          ProfitHierarchy
        INNER JOIN
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
            AS ProfitCenterFlattened
          ON
            ProfitHierarchy.mandt = ProfitCenterFlattened.mandt
            AND ProfitHierarchy.setclass = ProfitCenterFlattened.setclass
            AND ProfitHierarchy.subclass = ProfitCenterFlattened.subclass
            AND ProfitHierarchy.hiername = ProfitCenterFlattened.hiername
            AND ProfitHierarchy.parent = ProfitCenterFlattened.node
        WHERE
          (
            ProfitCenterFlattened.level != 0
            OR ProfitCenterFlattened.isleafnode
          )
          AND ProfitHierarchy.level != 1
          AND ProfitHierarchy.current_iteration != max_iterations --noqa:RF02
      )
    SELECT
      * EXCEPT (current_iteration)
    FROM
      ProfitHierarchy
  );

END;

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterMapping`(); --noqa:LT01
