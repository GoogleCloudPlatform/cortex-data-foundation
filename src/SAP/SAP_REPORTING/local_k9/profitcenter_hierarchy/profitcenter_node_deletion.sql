CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DeleteProfitCenterNode`(
  input_setclass STRING, input_subclass STRING, input_node STRING)
BEGIN
  DECLARE delete_node ARRAY< STRUCT < node STRING(50), hiername STRING(50), flag BOOL > >;
  DECLARE node_length INT64 DEFAULT NULL;
  DECLARE node_iteration INT64 DEFAULT 1;
  SET delete_node = ARRAY(SELECT AS STRUCT node, hiername, isleafnode
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
    WHERE setclass = input_setclass
      AND subclass = input_subclass
      AND node = input_node);
  SET node_length = ARRAY_LENGTH(delete_node);
    --recursively find all the children(if any) of node which are to be deleted
  WHILE node_iteration <= node_length DO
    IF delete_node[ORDINAL(node_iteration)].flag = FALSE
    THEN SET delete_node = ARRAY_CONCAT(
      delete_node,
      ARRAY(SELECT AS STRUCT node, hiername ,isleafnode
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
      WHERE setclass = input_setclass
        AND subclass = input_subclass
        AND hiername = delete_node[ORDINAL(node_iteration)].hiername
        AND parent = delete_node[ORDINAL(node_iteration)].node)); -- noqa: L003
    ELSE END IF;
    SET node_iteration = node_iteration + 1;
    SET node_length = ARRAY_LENGTH(delete_node);
  END WHILE;
  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
    WHERE node IN (SELECT node FROM UNNEST(delete_node));
END;
