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

CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterMapping`() --noqa:LT01
BEGIN
  --This procedure generates table having cost center mapped to cost center hierarchy nodes.
  DECLARE max_iterations INT64 DEFAULT NULL;

  SET max_iterations = (
    SELECT MAX(level) FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
  );

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers` (
    mandt STRING,
    setclass STRING,
    subclass STRING,
    hiername STRING,
    parent STRING,
    node STRING,
    costcenter STRING,
    level INT64,
    isleafnode BOOL) --noqa:LT02
  AS (
    WITH RECURSIVE
      CostHierarchy AS (
        --Insert leaf nodes with cost center mapping.
        SELECT
          mandt,
          setclass,
          subclass,
          hiername,
          parent,
          node,
          node AS costcenter,
          level,
          isleafnode,
          0 AS current_iteration
        FROM
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
        WHERE
          isleafnode
          AND level = 0

        UNION ALL

        --Insert cost center mapping by parent node combination for each level.
        SELECT
          CostHierarchy.mandt,
          CostHierarchy.setclass,
          CostHierarchy.subclass,
          CostHierarchy.hiername,
          CostCenterFlattened.parent,
          CostCenterFlattened.node,
          CostHierarchy.costcenter,
          CostCenterFlattened.level,
          CostCenterFlattened.isleafnode,
          CostHierarchy.current_iteration + 1 AS current_iteration
        FROM
          CostHierarchy
        INNER JOIN
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
            AS CostCenterFlattened
          ON
            CostHierarchy.mandt = CostCenterFlattened.mandt
            AND CostHierarchy.setclass = CostCenterFlattened.setclass
            AND CostHierarchy.subclass = CostCenterFlattened.subclass
            AND CostHierarchy.hiername = CostCenterFlattened.hiername
            AND CostHierarchy.parent = CostCenterFlattened.node
        WHERE
          (
            CostCenterFlattened.level != 0
            OR CostCenterFlattened.isleafnode
          )
          AND CostHierarchy.level != 1
          AND CostHierarchy.current_iteration != max_iterations --noqa:RF02
      )
    SELECT
      * EXCEPT (current_iteration)
    FROM
      CostHierarchy
  );

END;

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterMapping`(); --noqa:LT01
