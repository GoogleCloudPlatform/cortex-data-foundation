CREATE OR REPLACE PROCEDURE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHrFlattening`
(input_setclass STRING, input_subclass STRING)
BEGIN
  DECLARE level_id INT64 DEFAULT 0;
  DECLARE max_level INT64 DEFAULT 0;
  DECLARE hiername_length INT64 DEFAULT NULL;
  DECLARE hiername_iteration INT64 DEFAULT 0;
  DECLARE level_iteration INT64 DEFAULT 0;
  DECLARE node_array ARRAY< STRING >;
  DECLARE hiername_array ARRAY< STRING >;

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
  (
    mandt STRING,
    setclass STRING,
    subclass STRING,
    hiername STRING,
    node STRING,
    parent STRING,
    level INT64,
    isleafnode BOOL
  );

  {% if sql_flavour == 'ecc' -%}
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
    (mandt, setclass, subclass, hiername, node, parent, level, isleafnode)
    (
      --inserting root node(i.e., level 0 record) in the flattened table
      SELECT
        mandt,
        setclass,
        subclass,
        setname,
        setname,
        setname,
        0,
        FALSE
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.setheader`
      WHERE
        mandt = '{{ mandt }}'
        AND settype = 'S'
        AND setclass = input_setclass
        AND subclass = input_subclass
        AND setname NOT IN (
          SELECT subsetname
          FROM
            `{{ project_id_src }}.{{ dataset_cdc_processed }}.setnode`
          WHERE mandt = '{{ mandt }}' AND setclass = input_setclass)
    )
    UNION ALL
    (
      --inserting non leaf nodes in the flattened table
      SELECT
        mandt,
        setclass,
        subclass,
        NULL,
        subsetname,
        setname,
        NULL,
        FALSE
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.setnode`
      WHERE
        mandt = '{{ mandt }}'
        AND setclass = input_setclass
        AND subclass = input_subclass
    );

    --updating hierachy levels in flattened table
    WHILE (level_iteration = max_level) DO --noqa: disable=L003
      SET level_id = level_id + 1;

      SET node_array = ARRAY(
        SELECT DISTINCT node
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
        WHERE level = max_level
      );

      UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
      SET level = level_id
      WHERE level IS NULL AND parent IN UNNEST(node_array);

      SET level_iteration = level_iteration + 1;

      SET max_level = (
        SELECT MAX(level)
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
      );
    END WHILE;
    --noqa: enable=all

    --inserting leaf nodes with valsign = 'I' in the flattened table
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
    (mandt, setclass, subclass, hiername, node, parent, level, isleafnode)
    SELECT
      mandt,
      setclass,
      subclass,
      NULL,
      node,
      setname,
      0,
      TRUE
    FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.setleaf`
    CROSS JOIN
    (
      SELECT DISTINCT kostl AS node
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks`
      WHERE mandt = '{{ mandt }}'
        AND kokrs = input_subclass
        AND datbi >= '9999-12-31'
    )
    WHERE mandt = '{{ mandt }}'
      AND setclass = input_setclass
      AND subclass = input_subclass
      AND valsign = 'I'
      AND node BETWEEN valfrom AND valto;

    --excluding leaf nodes with valsign = 'E' from the flattened table
    DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
    WHERE mandt='{{ mandt }}'
      AND setclass = input_setclass
      AND subclass = input_subclass
      AND node IN (
        SELECT node
        FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.setleaf`
        CROSS JOIN
        (
          SELECT
            DISTINCT kostl AS node
          FROM
            `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks`
          WHERE
            mandt = '{{ mandt }}'
            AND kokrs = input_subclass
            AND datbi >= '9999-12-31'
        )
        WHERE
          mandt = '{{ mandt }}'
          AND setclass = input_setclass
          AND subclass = input_subclass
          AND valsign = 'E'
          AND node BETWEEN valfrom AND valto
      );

    --updating hierachy name in flattened table
    SET hiername_array = ARRAY(
      SELECT DISTINCT hiername
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
      WHERE level = 0 AND NOT isleafnode
    );

    SET hiername_length = ARRAY_LENGTH(hiername_array);

    WHILE (hiername_iteration < hiername_length) DO
      SET level_iteration = 1;

      SET max_level = (
        SELECT MAX(level)
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
      );

      WHILE (level_iteration <= max_level) DO
        --updating hierarchy name for non-leaf nodes
        SET node_array = ARRAY(
          SELECT DISTINCT node
          FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
          WHERE level = level_iteration - 1
            AND hiername = hiername_array[hiername_iteration]
        );

        UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
        SET hiername = (
          SELECT DISTINCT hiername
          FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
          WHERE level = level_iteration - 1
            AND node IN UNNEST(node_array)
        ) WHERE parent IN UNNEST(node_array);

        SET level_iteration = level_iteration + 1;
      END WHILE;

      --updating hierarchy name for leaf nodes
      SET node_array = ARRAY(
        SELECT DISTINCT node
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
        WHERE isleafnode
          AND parent IN (
            SELECT DISTINCT node
            FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
            WHERE level = max_level
              AND hiername = hiername_array[hiername_iteration]
          )
      );

      UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
      SET hiername = hiername_array[hiername_iteration]
      WHERE node IN UNNEST(node_array);

      SET hiername_iteration = hiername_iteration + 1;
    END WHILE; --noqa: L003

  {% else -%}
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
    (mandt, setclass, subclass, hiername, node, parent, level, isleafnode)
    (
      --inserting non leaf nodes in the flattened table
      SELECT
        mandt,
        setclass,
        subclass,
        hierbase,
        CAST(succ AS STRING),
        CAST(pred AS STRING),
        hlevel,
        FALSE
      FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.sethanahier0101`
      WHERE
        mandt = '{{ mandt }}'
        AND (hlevel !=0 OR succ = -1)
        AND setclass = input_setclass
        AND subclass = input_subclass
    )
    UNION ALL
    (
      --inserting leaf nodes in the flattened table
      SELECT
        mandt,
        setclass,
        subclass,
        hierbase,
        node,
        CAST(pred AS STRING),
        hlevel,
        TRUE
      FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.sethanahier0101`
      CROSS JOIN
      (
        SELECT DISTINCT kostl AS node
        FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks`
        WHERE mandt = '{{ mandt }}'
          AND kokrs = input_subclass
          AND datbi >= '9999-12-31'
      )
      WHERE
        mandt = '{{ mandt }}'
        AND hlevel = 0
        AND succ != -1
        AND setclass = input_setclass
        AND subclass = input_subclass
        AND node BETWEEN value_from AND value_to
    );

  {% endif -%}
END;

--## CORTEX-CUSTOMER: Update the inputs(setclass, subclass)
--for cost center flattening based on your requirement
--noqa: disable=L003
CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHrFlattening`(
  {% if sql_flavour == 'ecc' -%}
    '0101', 'C001' -- noqa: L048
  {% else -%}
    '0101', 'A000'
  {% endif -%}
);
--noqa: enable=all
