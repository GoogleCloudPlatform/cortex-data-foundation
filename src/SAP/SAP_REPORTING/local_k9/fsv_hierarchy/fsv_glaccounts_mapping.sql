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

CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVGLAccountMapping`() --noqa:LT01
BEGIN
  --This procedure generates table having glaccounts mapped to fsv hierarchy nodes.

  --(Re)create table and insert leaf nodes with GL Account mapping
  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` (
    mandt STRING,
    chartofaccounts STRING,
    hiername STRING,
    {% if sql_flavour == 's4' -%}
    hierarchyversion STRING,
    {% endif -%}
    parent STRING,
    node STRING,
    {% if sql_flavour == 'ecc' -%} --noqa:LT02
    ergsl STRING, --noqa:LT02
    {% else -%}
    nodevalue STRING,
    {% endif -%}
    glaccount STRING,
    level STRING,
    isleafnode BOOL
  ) AS
  SELECT
    mandt,
    chartofaccounts,
    hiername,
    {% if sql_flavour == 's4' -%}
    hierarchyversion,
    {% endif -%}
    parent,
    node,
    {% if sql_flavour == 'ecc' -%} --noqa:disable=LT02
    ergsl,
    node AS glaccount,
    {% else -%}
    nodevalue,
    nodevalue AS glaccount,
    {% endif -%}
    level, --noqa: enable=all
    isleafnode
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
  WHERE --noqa: disable=LT02
    isleafnode
    {% if sql_flavour == 'ecc' -%}
    AND node = ergsl;
    {% else -%}
    AND node = CONCAT('1', nodevalue);
    {% endif -%}
    --noqa: enable=all

  -- Create temp table noting parents and relevant glaccounts

  CREATE TEMP TABLE GLAccountsByLeafParents AS (
    SELECT DISTINCT
      level,
      parent AS leaf_parent,
      glaccount
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`
  );

  -- 1. From leaf node, recursively gather all parents
  -- 2. For all parents gathered, append relevant glaccounts
  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`
  (
    mandt,
    chartofaccounts,
    hiername,
    {% if sql_flavour == 's4' -%}
    hierarchyversion,
    {% endif -%}
    parent,
    node,
    {% if sql_flavour == 'ecc' -%} --noqa:disable=LT02
    ergsl,
    {% else -%}
    nodevalue,
    {% endif -%} --noqa: enable=all
    glaccount,
    level,
    isleafnode
  )
  WITH RECURSIVE
    Parents AS (
      SELECT DISTINCT
        leaf_parent,
        leaf_parent AS parent,
        level
      FROM
        GLAccountsByLeafParents

      UNION ALL

      SELECT
        Parents.leaf_parent,
        FSVFlattened.parent,
        FSVFlattened.level
      FROM
        Parents
      INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened` AS FSVFlattened
        ON Parents.parent = FSVFlattened.node
      WHERE
        {% if sql_flavour == 'ecc' -%} --noqa: disable=LT02
        FSVFlattened.level != '01'
        {% else -%}
        FSVFlattened.level != '000001'
        {% endif -%}
        --noqa: enable=all
    ) --noqa:LT07

  SELECT
    FSVFlattened.mandt,
    FSVFlattened.chartofaccounts,
    FSVFlattened.hiername,
    {% if sql_flavour == 's4' -%}
    FSVFlattened.hierarchyversion,
    {% endif -%}
    FSVFlattened.parent,
    FSVFlattened.node,
    {% if sql_flavour == 'ecc' -%} --noqa:disable=LT02
    FSVFlattened.ergsl,
    {% else -%}
    FSVFlattened.nodevalue,
    {% endif -%} --noqa: enable=all
    GLAccountsByLeafParents.glaccount,
    FSVFlattened.level,
    FSVFlattened.isleafnode
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened` AS FSVFlattened
  INNER JOIN
    Parents
    ON
      FSVFlattened.node = Parents.parent
  INNER JOIN
    GLAccountsByLeafParents
    ON
      Parents.leaf_parent = GLAccountsByLeafParents.leaf_parent
  WHERE
    {% if sql_flavour == 'ecc' -%} --noqa: disable=LT02
    FSVFlattened.level != '01';
    {% else -%}
    FSVFlattened.level != '000001';
    {% endif -%} --noqa: enable=all

END;

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVGLAccountMapping`(); --noqa:LT01
