CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DeleteFSVNode`(
  input_chartofaccounts STRING, input_node STRING)
BEGIN
  DECLARE delete_node ARRAY< STRUCT < node STRING(50), flag BOOL > >;
  DECLARE len INT64 DEFAULT NULL;
  DECLARE i INT64 DEFAULT 1;

  SET delete_node = ARRAY(SELECT AS STRUCT node, isleafnode
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
    WHERE chartofaccounts = input_chartofaccounts
      AND node = input_node);
  SET len = ARRAY_LENGTH(delete_node);
    --recursively find all the children(if any) of node which are to be deleted
  WHILE i <= len DO
    IF delete_node[ORDINAL(i)].flag = FALSE
    THEN SET delete_node = ARRAY_CONCAT(
      delete_node,
      ARRAY(SELECT AS STRUCT node ,isleafnode
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
      WHERE chartofaccounts = input_chartofaccounts
        AND parent = delete_node[ORDINAL(i)].node));
    ELSE END IF;
    SET i=i+1;
    SET len= ARRAY_LENGTH(delete_node);
  END WHILE;
  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
    WHERE node IN (SELECT node FROM UNNEST(delete_node));
END;

--## CORTEX-CUSTOMER: Update the inputs(chart of accounts, node) to delete specific node
--and its folllowing hierarchy based on your requirement
CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DeleteFSVNode`(NULL, NULL);
