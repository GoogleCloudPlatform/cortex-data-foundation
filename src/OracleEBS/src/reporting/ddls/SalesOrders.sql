-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- SalesOrders reporting table.

WITH
  ReturnLines AS (
    SELECT
      REFERENCE_HEADER_ID,
      REFERENCE_LINE_ID,
      ARRAY_AGG(LINE_ID) AS RETURN_LINE_IDS
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderLines`
    WHERE LINE_CATEGORY_CODE = 'RETURN'
    GROUP BY 1, 2
  ),
  BackorderedDeliveryDetails AS (
    SELECT
      SOURCE_LINE_ID,
      LOGICAL_OR(RELEASED_STATUS = 'B') AS IS_BACKORDERED
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.DeliveryDetails`
    GROUP BY 1
  ),
  LatestCancelReasons AS (
    SELECT
      ENTITY_ID,
      MAX(LAST_UPDATE_TS) AS LAST_REASON_TS
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderReasons`
    WHERE
      ENTITY_CODE = 'LINE'
      AND REASON_TYPE = 'CANCEL_CODE'
    GROUP BY 1
  ),
  OrderLineReasons AS (
    SELECT
      OrderReasons.ENTITY_ID,
      ARRAY_AGG(
        STRUCT(LookupValue.LOOKUP_CODE AS CODE, LookupValue.MEANING, LookupValue.LANGUAGE)
      ) AS CANCEL_REASON
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderReasons` AS OrderReasons
    -- Get the latest cancel reason for each order line.
    INNER JOIN LatestCancelReasons
      ON
        OrderReasons.ENTITY_ID = LatestCancelReasons.ENTITY_ID
        AND OrderReasons.LAST_UPDATE_TS = LatestCancelReasons.LAST_REASON_TS
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LookupValueMD` AS LookupValue
      ON
        OrderReasons.REASON_TYPE = LookupValue.LOOKUP_TYPE
        AND OrderReasons.REASON_CODE = LookupValue.LOOKUP_CODE
    GROUP BY 1
  ),
  OrderLines AS (
    SELECT
      * REPLACE (
        COALESCE(ACTUAL_FULFILLMENT_DATE, FULFILLMENT_DATE) AS FULFILLMENT_DATE
      ),
      FLOW_STATUS_CODE IN ('ENTERED', 'BOOKED') AS IS_BOOKING,
      BOOKED_FLAG = 'Y' AS IS_BOOKED,
      FLOW_STATUS_CODE NOT IN ('ENTERED', 'BOOKED', 'CLOSED', 'CANCELLED') AS IS_BACKLOG,
      OPEN_FLAG = 'Y' AND FLOW_STATUS_CODE NOT IN ('CLOSED', 'CANCELLED') AS IS_OPEN,
      CANCELLED_FLAG = 'Y' OR FLOW_STATUS_CODE = 'CANCELLED' AS IS_CANCELLED,
      COALESCE(FULFILLED_FLAG = 'Y', FALSE) AS IS_FULFILLED,
      IF(FLOW_STATUS_CODE IN ('ENTERED', 'BOOKED'), ORDERED_QUANTITY, 0) AS BOOKING_QUANTITY,
      IF(
        FLOW_STATUS_CODE NOT IN ('ENTERED', 'BOOKED', 'CLOSED', 'CANCELLED'),
        ORDERED_QUANTITY - FULFILLED_QUANTITY,
        0
      ) AS BACKLOG_QUANTITY
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderLines`
  ),
  -- Process fields that only apply at the line level.
  Lines AS (
    SELECT
      OrderLines.HEADER_ID,
      OrderLines.LINE_ID,
      OrderLines.LINE_NUMBER,
      OrderLines.LINE_CATEGORY_CODE,
      OrderLines.FLOW_STATUS_CODE AS LINE_STATUS,
      OrderLines.SHIP_FROM_ORG_ID AS ITEM_ORGANIZATION_ID,
      OrgMD.ORGANIZATION_NAME AS ITEM_ORGANIZATION_NAME,
      OrderLines.INVENTORY_ITEM_ID,
      ItemMD.ITEM_PART_NUMBER,
      ItemMD.ITEM_DESCRIPTIONS,
      ItemMD.ITEM_CATEGORIES,
      ItemMD.WEIGHT_UOM_CODE AS WEIGHT_UOM,
      ItemMD.UNIT_WEIGHT,
      OrderLines.ORDER_QUANTITY_UOM AS QUANTITY_UOM,
      OrderLines.SHIPPED_QUANTITY,
      OrderLines.ORDERED_QUANTITY,
      OrderLines.FULFILLED_QUANTITY,
      OrderLines.INVOICED_QUANTITY,
      OrderLines.CANCELLED_QUANTITY,
      OrderLines.BOOKING_QUANTITY,
      OrderLines.BACKLOG_QUANTITY,
      OrderLines.ORDERED_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS ORDERED_AMOUNT,
      OrderLines.SHIPPED_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS SHIPPED_AMOUNT,
      OrderLines.FULFILLED_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS FULFILLED_AMOUNT,
      OrderLines.INVOICED_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS INVOICED_AMOUNT,
      OrderLines.BOOKING_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS BOOKING_AMOUNT,
      OrderLines.BACKLOG_QUANTITY * OrderLines.UNIT_SELLING_PRICE AS BACKLOG_AMOUNT,
      OrderLines.UNIT_COST,
      OrderLines.UNIT_LIST_PRICE,
      OrderLines.UNIT_SELLING_PRICE,
      OrderLines.ORDERED_QUANTITY * ItemMD.UNIT_WEIGHT AS ORDERED_WEIGHT,
      OrderLines.SHIPPED_QUANTITY * ItemMD.UNIT_WEIGHT AS SHIPPED_WEIGHT,
      OrderLines.SCHEDULE_SHIP_DATE,
      OrderLines.ACTUAL_SHIP_DATE,
      OrderLines.REQUEST_DATE,
      OrderLines.PROMISE_DATE,
      OrderLines.FULFILLMENT_DATE,
      OrderLines.IS_BOOKING,
      OrderLines.IS_BOOKED,
      OrderLines.IS_BACKLOG,
      BackorderedDeliveryDetails.IS_BACKORDERED,
      OrderLines.IS_OPEN,
      OrderLines.IS_CANCELLED,
      OrderLineReasons.CANCEL_REASON,
      OrderLines.IS_FULFILLED,
      ReturnLines.REFERENCE_LINE_ID IS NOT NULL AS HAS_RETURN,
      ReturnLines.RETURN_LINE_IDS,
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.IsOnTime`(
        OrderLines.REQUEST_DATE, OrderLines.FULFILLMENT_DATE)
        AS IS_FULFILLED_BY_REQUEST_DATE,
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.IsOnTime`(
        OrderLines.PROMISE_DATE, OrderLines.FULFILLMENT_DATE)
        AS IS_FULFILLED_BY_PROMISE_DATE,
      DATE_DIFF(OrderLines.FULFILLMENT_DATE, OrderLines.REQUEST_DATE, DAY)
        AS FULFILLMENT_DAYS_AFTER_REQUEST_DATE,
      DATE_DIFF(OrderLines.FULFILLMENT_DATE, OrderLines.PROMISE_DATE, DAY)
        AS FULFILLMENT_DAYS_AFTER_PROMISE_DATE,
      OrderLines.CREATION_TS,
      OrderLines.LAST_UPDATE_TS
    FROM OrderLines
    LEFT OUTER JOIN ReturnLines
      ON
        OrderLines.HEADER_ID = ReturnLines.REFERENCE_HEADER_ID
        AND OrderLines.LINE_ID = ReturnLines.REFERENCE_LINE_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrganizationMD` AS OrgMD
      ON OrderLines.SHIP_FROM_ORG_ID = OrgMD.ORGANIZATION_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.ItemMD` AS ItemMD
      ON
        OrderLines.INVENTORY_ITEM_ID = ItemMD.INVENTORY_ITEM_ID
        AND OrderLines.SHIP_FROM_ORG_ID = ItemMD.ORGANIZATION_ID
    LEFT OUTER JOIN OrderLineReasons
      ON OrderLines.LINE_ID = OrderLineReasons.ENTITY_ID
    LEFT OUTER JOIN BackorderedDeliveryDetails
      ON OrderLines.LINE_ID = BackorderedDeliveryDetails.SOURCE_LINE_ID
  ),
  OrderHolds AS (
    SELECT
      -- The source has holds at the line level where applicable, but we are aggregating those up
      -- to the header.
      HEADER_ID,
      LOGICAL_OR(RELEASED_FLAG = 'N') AS IS_HELD
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderHolds`
    GROUP BY 1
  ),
  -- Process fields that only apply at the header level.
  Headers AS (
    SELECT
      OrderHeaders.HEADER_ID,
      OrderHeaders.ORDER_NUMBER,
      OrderHeaders.ORDER_CATEGORY_CODE,
      -- The ordered date may be NULL before an order has been booked, but we would still like to
      -- have a value in those cases to include them in the aggregation table.
      COALESCE(OrderHeaders.ORDERED_DATE, DATE(OrderHeaders.CREATION_TS)) AS ORDERED_DATE,
      CalDateMD.CalMonth AS ORDERED_MONTH_NUM,
      CalDateMD.CalQuarter AS ORDERED_QUARTER_NUM,
      CalDateMD.CalYear AS ORDERED_YEAR_NUM,
      FiscalDateMD.PERIOD_TYPE AS FISCAL_PERIOD_TYPE,
      FiscalDateMD.PERIOD_SET_NAME AS FISCAL_PERIOD_SET_NAME,
      FiscalDateMD.PERIOD_NAME AS FISCAL_PERIOD_NAME,
      FiscalDateMD.PERIOD_NUM AS FISCAL_PERIOD_NUM,
      FiscalDateMD.QUARTER_NUM AS FISCAL_QUARTER_NUM,
      FiscalDateMD.YEAR_NUM AS FISCAL_YEAR_NUM,
      OrderHeaders.REQUEST_DATE,
      OrderHeaders.BOOKED_DATE,
      OrderHeaders.BILL_TO_SITE_USE_ID,
      BilledCustomerMD.ACCOUNT_NUMBER AS BILL_TO_CUSTOMER_NUMBER,
      BilledCustomerMD.ACCOUNT_NAME AS BILL_TO_CUSTOMER_NAME,
      BilledCustomerMD.COUNTRY_NAME AS BILL_TO_CUSTOMER_COUNTRY,
      OrderHeaders.SOLD_TO_SITE_USE_ID,
      SoldCustomerMD.ACCOUNT_NUMBER AS SOLD_TO_CUSTOMER_NUMBER,
      SoldCustomerMD.ACCOUNT_NAME AS SOLD_TO_CUSTOMER_NAME,
      SoldCustomerMD.COUNTRY_NAME AS SOLD_TO_CUSTOMER_COUNTRY,
      OrderHeaders.SHIP_TO_SITE_USE_ID,
      ShippedCustomerMD.ACCOUNT_NUMBER AS SHIP_TO_CUSTOMER_NUMBER,
      ShippedCustomerMD.ACCOUNT_NAME AS SHIP_TO_CUSTOMER_NAME,
      ShippedCustomerMD.COUNTRY_NAME AS SHIP_TO_CUSTOMER_COUNTRY,
      OrderHeaders.BUSINESS_UNIT_ID,
      BusinessUnitMD.BUSINESS_UNIT_NAME,
      BusinessUnitMD.LEDGER_ID,
      LedgerMD.LEDGER_NAME,
      OrderHeaders.FLOW_STATUS_CODE AS HEADER_STATUS,
      SalesRepMD.Name AS SALES_REP,
      OrderHeaders.ORDER_SOURCE_ID,
      OrderSourceMD.NAME AS ORDER_SOURCE_NAME,
      OrderHeaders.TRANSACTIONAL_CURR_CODE AS CURRENCY_CODE,
      OrderHeaders.ORDER_SOURCE_ID = 10 AS IS_INTERCOMPANY,
      OrderHeaders.OPEN_FLAG = 'Y' AND OrderHeaders.FLOW_STATUS_CODE NOT IN ('CLOSED', 'CANCELLED')
        AS IS_OPEN,
      OrderHeaders.BOOKED_FLAG = 'Y' OR OrderHeaders.FLOW_STATUS_CODE = 'BOOKED' AS IS_BOOKED,
      OrderHeaders.CANCELLED_FLAG = 'Y' OR OrderHeaders.FLOW_STATUS_CODE = 'CANCELLED'
        AS IS_CANCELLED,
      COALESCE(OrderHolds.IS_HELD, FALSE) AS IS_HELD,
      OrderHolds.HEADER_ID IS NOT NULL AS HAS_HOLD,
      OrderHeaders.CREATION_TS,
      OrderHeaders.LAST_UPDATE_TS
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderHeaders` AS OrderHeaders
    LEFT OUTER JOIN OrderHolds
      ON OrderHeaders.HEADER_ID = OrderHolds.HEADER_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesRepMD` AS SalesRepMD
      ON
        OrderHeaders.SALESREP_ID = SalesRepMD.SALESREP_ID
        AND OrderHeaders.BUSINESS_UNIT_ID = SalesRepMD.BUSINESS_UNIT_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.OrderSourceMD` AS OrderSourceMD
      ON OrderHeaders.ORDER_SOURCE_ID = OrderSourceMD.ORDER_SOURCE_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS BilledCustomerMD
      ON OrderHeaders.BILL_TO_SITE_USE_ID = BilledCustomerMD.SITE_USE_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS SoldCustomerMD
      ON OrderHeaders.SOLD_TO_SITE_USE_ID = SoldCustomerMD.SITE_USE_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS ShippedCustomerMD
      ON OrderHeaders.SHIP_TO_SITE_USE_ID = ShippedCustomerMD.SITE_USE_ID
    LEFT OUTER JOIN
      `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalDateMD
      ON COALESCE(OrderHeaders.ORDERED_DATE, DATE(OrderHeaders.CREATION_TS)) = CalDateMD.DATE
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.BusinessUnitMD` AS BusinessUnitMD
      ON OrderHeaders.BUSINESS_UNIT_ID = BusinessUnitMD.BUSINESS_UNIT_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS LedgerMD
      ON BusinessUnitMD.LEDGER_ID = LedgerMD.LEDGER_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.FiscalDateMD` AS FiscalDateMD
      ON
        OrderHeaders.ORDERED_DATE = FiscalDateMD.FISCAL_DATE
        AND LedgerMD.PERIOD_SET_NAME = FiscalDateMD.PERIOD_SET_NAME
        AND LedgerMD.PERIOD_TYPE = FiscalDateMD.PERIOD_TYPE
  ),
  -- Process line level fields that also depend on header fields.
  LinesWithHeader AS (
    SELECT
      Headers.HEADER_ID,
      IF(LOGICAL_AND(Lines.IS_FULFILLED), MAX(Lines.FULFILLMENT_DATE), NULL) AS FULFILLMENT_DATE,
      LOGICAL_AND(Lines.IS_FULFILLED) AS IS_FULFILLED,
      LOGICAL_OR(Lines.IS_BACKORDERED) AS HAS_BACKORDER,
      LOGICAL_OR(Lines.HAS_RETURN) AS HAS_RETURN_LINE,
      LOGICAL_OR(Lines.IS_CANCELLED) AS HAS_CANCELLED,
      COUNT(Lines.LINE_ID) AS NUM_LINES,
      COUNTIF(Lines.IS_FULFILLED_BY_REQUEST_DATE) AS NUM_LINES_FULFILLED_BY_REQUEST_DATE,
      COUNTIF(Lines.IS_FULFILLED_BY_PROMISE_DATE) AS NUM_LINES_FULFILLED_BY_PROMISE_DATE,
      SUM(Lines.ORDERED_AMOUNT) AS TOTAL_ORDERED_AMOUNT,
      SUM(IF(Lines.LINE_CATEGORY_CODE = 'RETURN', 0, Lines.ORDERED_AMOUNT))
        AS TOTAL_SALES_ORDERED_AMOUNT,
      ARRAY_AGG((
        SELECT AS STRUCT
          Lines.* EXCEPT (HEADER_ID),
          DATE_DIFF(Lines.FULFILLMENT_DATE, Headers.ORDERED_DATE, DAY) AS CYCLE_TIME_DAYS
      )) AS LINES
    FROM Headers
    LEFT OUTER JOIN Lines
      USING (HEADER_ID)
    GROUP BY 1
  )
SELECT
  Headers.*,
  LinesWithHeader.* EXCEPT (HEADER_ID)
FROM Headers
LEFT OUTER JOIN LinesWithHeader
  USING (HEADER_ID)
