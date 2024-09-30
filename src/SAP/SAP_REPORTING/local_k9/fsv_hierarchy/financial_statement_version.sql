CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVHrFlattening`(
  input_chartofaccounts STRING, input_hiername STRING)
BEGIN
  {% if sql_flavour == 'ecc' -%}
    --This procedure generates the flattened financial statement version hierarchy table.
    DECLARE parent_array ARRAY< STRING >;
    CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
    (
      mandt STRING,
      chartofaccounts STRING,
      hiername STRING,
      parent STRING,
      node STRING,
      ergsl STRING,
      level STRING,
      isleafnode BOOL
    );

    --inserting non-leaf/parent nodes in the table
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
    (mandt, chartofaccounts, hiername, parent, node, ergsl, level, isleafnode)
    SELECT
      mandt,
      input_chartofaccounts,
      versn,
      IF(type = 'R', versn, parent) AS parent,
      id,
      ergsl,
      stufe,
      IF(child IS NULL, TRUE, FALSE) AS isleafnode
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.fagl_011pc`
    WHERE
      mandt = '{{ mandt }}'
      AND versn = input_hiername;

    SET parent_array = ARRAY(
      SELECT DISTINCT parent FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`);

    CREATE OR REPLACE TEMP TABLE LeafNodeDetails AS (
      SELECT
        node,
        ergsl,
        CAST(level AS INT64) AS level
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
      WHERE
        node NOT IN UNNEST(parent_array)
        AND isleafnode IS FALSE);

    --inserting GL Accounts(leaf nodes) in table
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
    (mandt, chartofaccounts, hiername, parent, node, ergsl, level, isleafnode)
    SELECT
      fagl_011zc.mandt,
      fagl_011zc.ktopl,
      fagl_011zc.versn,
      leafnode.node,
      gl_account,
      gl_account,
      IF(leafnode.level + 1 < 10,
        CONCAT('0', CAST(leafnode.level + 1 AS STRING)),
        CAST(leafnode.level + 1 AS STRING)
      ) AS level,
      TRUE AS isleafnode
    FROM (
      SELECT mandt, ktopl, versn, vonkt, biskt, ergsl
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.fagl_011zc`
      WHERE mandt = '{{ mandt }}'
        AND ktopl = input_chartofaccounts
        AND versn = input_hiername) AS fagl_011zc
    INNER JOIN LeafNodeDetails AS leafnode
      ON
        fagl_011zc.ergsl = leafnode.ergsl,
      UNNEST(
        ARRAY(SELECT saknr FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.ska1`
          WHERE mandt = '{{ mandt }}'
            AND saknr BETWEEN vonkt AND biskt
            AND ktopl = input_chartofaccounts
        )) AS gl_account;

  {% else -%}
  DECLARE hierarchy_version ARRAY < STRUCT < HryVer STRING (20), HryTyp STRING(20) > >;
  DECLARE hryversion STRING DEFAULT NULL;
  DECLARE hrytype STRING DEFAULT NULL;
  DECLARE node_array ARRAY < STRING >;
  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
  (
    mandt STRING,
    chartofaccounts STRING,
    hiername STRING,
    hierarchyversion STRING,
    parent STRING,
    node STRING,
    nodevalue STRING,
    level STRING,
    isleafnode BOOL
  );

  SET hierarchy_version = ARRAY(SELECT DISTINCT AS STRUCT hryver,hrytyp
    FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.hrrp_directory`
    WHERE mandt='{{ mandt }}' AND hryid= input_hiername
    AND hryvalto =
    (SELECT max(hryvalto) FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.hrrp_directory`
     WHERE  mandt='{{ mandt }}' AND hryid= input_hiername));

  SET hryversion = (SELECT HryVer FROM unnest(hierarchy_version));
  SET hrytype = (SELECT lower(HryTyp) FROM unnest(hierarchy_version));

  --inserting nodes in the flattened table
  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
  (mandt, chartofaccounts, hiername,hierarchyversion, parent, node, nodevalue, level, isleafnode)
    SELECT
      mandt,
      input_chartofaccounts,
      hryid,
      hryver,
      IF(nodetype = 'R', nodevalue, parnode) AS parent,
      hrynode,
      nodevalue,
      hrylevel,
      IF(nodetype = 'L', TRUE, FALSE) AS isleafnode
    FROM
     `{{ project_id_src }}.{{ dataset_cdc_processed }}.hrrp_node`
    WHERE
      mandt = '{{ mandt }}'
      AND hryid = input_hiername
      AND (nodetype in ('R','N') OR (nodetype='L' AND nodecls=input_chartofaccounts))
      AND hryver = hryversion;

  SET node_array = ARRAY(SELECT DISTINCT parnode
    FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.hrrp_node`
    WHERE
      mandt = '{{ mandt }}'
      AND hryid = input_hiername
      AND nodetype in ('N','L','R')
      AND hryver = hryversion);

  UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
  SET isleafnode = TRUE
  WHERE node NOT IN (SELECT * FROM UNNEST(node_array))
    AND isleafnode = FALSE;
  {% endif -%}
END;

--## CORTEX-CUSTOMER: Update the inputs(chart of accounts, hierarchy name) for fsv flattening
-- based on your requirement
CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVHrFlattening`(
  {% if sql_flavour == 'ecc' -%}
    'CA01', 'FPA1' -- noqa: L048
  {% else -%}
    'YCOA', 'FPA1'
  {% endif -%}
  );
