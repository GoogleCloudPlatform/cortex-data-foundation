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

-- SalesDeliveryDetails reporting table.

WITH
  Orders AS (
    SELECT
      H.HEADER_ID,
      H.BUSINESS_UNIT_ID,
      H.BUSINESS_UNIT_NAME,
      H.LEDGER_ID,
      H.LEDGER_NAME,
      H.ORDER_NUMBER,
      H.ORDERED_DATE,
      H.HEADER_STATUS,
      H.ORDER_SOURCE_ID,
      H.ORDER_SOURCE_NAME,
      H.SHIP_TO_SITE_USE_ID,
      H.SHIP_TO_CUSTOMER_NUMBER,
      H.SHIP_TO_CUSTOMER_NAME,
      H.SHIP_TO_CUSTOMER_COUNTRY,
      H.IS_OPEN,
      H.IS_BOOKED,
      H.CURRENCY_CODE,
      L.LINE_ID,
      L.LINE_NUMBER,
      L.LINE_STATUS,
      L.ITEM_ORGANIZATION_ID,
      L.ITEM_ORGANIZATION_NAME,
      L.INVENTORY_ITEM_ID,
      L.ITEM_PART_NUMBER,
      L.ITEM_DESCRIPTIONS,
      L.ITEM_CATEGORIES,
      L.REQUEST_DATE,
      L.PROMISE_DATE,
      L.FULFILLMENT_DATE,
      L.SCHEDULE_SHIP_DATE,
      L.ACTUAL_SHIP_DATE,
      L.UNIT_LIST_PRICE,
      L.UNIT_SELLING_PRICE,
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.IsOnTime`(
        L.SCHEDULE_SHIP_DATE, L.ACTUAL_SHIP_DATE)
        AS IS_SHIPPED_ON_TIME,
      DATE_DIFF(L.ACTUAL_SHIP_DATE, L.SCHEDULE_SHIP_DATE, DAY) AS DAYS_SHIPPED_AFTER_SCHEDULE_DATE,
      L.QUANTITY_UOM,
      L.ORDERED_QUANTITY
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesOrders` AS H
    LEFT JOIN UNNEST(H.LINES) AS L
  )
SELECT
  Deliveries.DELIVERY_ID,
  Details.DELIVERY_DETAIL_ID,
  Orders.SHIP_TO_SITE_USE_ID,
  Orders.SHIP_TO_CUSTOMER_NUMBER,
  Orders.SHIP_TO_CUSTOMER_NAME,
  Orders.SHIP_TO_CUSTOMER_COUNTRY,
  Orders.BUSINESS_UNIT_ID,
  Orders.BUSINESS_UNIT_NAME,
  Orders.LEDGER_ID,
  Orders.LEDGER_NAME,
  Orders.ITEM_ORGANIZATION_ID,
  Orders.ITEM_ORGANIZATION_NAME,
  Deliveries.STATUS_CODE AS DELIVERY_STATUS_CODE,
  Deliveries.CONFIRM_DATE AS DELIVERED_DATE,
  CalDateMD.CalMonth AS DELIVERED_MONTH_NUM,
  CalDateMD.CalQuarter AS DELIVERED_QUARTER_NUM,
  CalDateMD.CalYear AS DELIVERED_YEAR_NUM,
  FiscalDateMD.PERIOD_TYPE AS FISCAL_PERIOD_TYPE,
  FiscalDateMD.PERIOD_SET_NAME AS FISCAL_PERIOD_SET_NAME,
  FiscalDateMD.PERIOD_NAME AS FISCAL_PERIOD_NAME,
  FiscalDateMD.PERIOD_NUM AS FISCAL_PERIOD_NUM,
  FiscalDateMD.QUARTER_NUM AS FISCAL_QUARTER_NUM,
  FiscalDateMD.YEAR_NUM AS FISCAL_YEAR_NUM,
  Details.SOURCE_HEADER_ID AS ORDER_HEADER_ID,
  Details.SOURCE_LINE_ID AS ORDER_LINE_ID,
  Orders.ORDER_NUMBER,
  Orders.LINE_NUMBER AS ORDER_LINE_NUMBER,
  Orders.HEADER_STATUS AS ORDER_HEADER_STATUS,
  Orders.ORDER_SOURCE_ID,
  Orders.ORDER_SOURCE_NAME,
  Orders.IS_OPEN AS IS_OPEN_ORDER,
  Orders.IS_BOOKED AS IS_BOOKED_ORDER,
  Orders.LINE_STATUS AS ORDER_LINE_STATUS,
  Orders.ORDERED_DATE,
  Orders.REQUEST_DATE,
  Orders.PROMISE_DATE,
  Orders.FULFILLMENT_DATE,
  Orders.SCHEDULE_SHIP_DATE,
  Orders.ACTUAL_SHIP_DATE,
  Orders.INVENTORY_ITEM_ID,
  Orders.ITEM_PART_NUMBER,
  Orders.ITEM_DESCRIPTIONS,
  Orders.ITEM_CATEGORIES,
  Orders.UNIT_LIST_PRICE,
  Orders.UNIT_SELLING_PRICE,
  Orders.CURRENCY_CODE,
  Details.RELEASED_STATUS,
  CASE UPPER(Details.RELEASED_STATUS)
    WHEN 'B' THEN 'Backordered'
    WHEN 'C' THEN 'Shipped'
    WHEN 'D' THEN 'Cancelled'
    WHEN 'N' THEN 'Not Ready for Release'
    WHEN 'R' THEN 'Ready to Release'
    WHEN 'S' THEN 'Released to Warehouse'
    WHEN 'Y' THEN 'Staged'
    WHEN 'E' THEN 'Replenishment Requested'
    WHEN 'F' THEN 'Replenishment Completed'
    WHEN 'K' THEN 'Planned for Crossdocking'
    WHEN 'O' THEN 'Not Shipped'
    WHEN 'I' THEN 'Interfaced'
    WHEN 'X' THEN 'Not Applicable'
    ELSE 'Not Applicable'
    END AS RELEASE_STATUS_DESCRIPTION, -- noqa: LT02
  Details.SHIP_METHOD_CODE,
  Details.FREIGHT_TERMS_CODE,
  Orders.QUANTITY_UOM,
  Orders.ORDERED_QUANTITY,
  Details.CANCELLED_QUANTITY,
  Details.REQUESTED_QUANTITY,
  Details.SRC_REQUESTED_QUANTITY,
  Details.SHIPPED_QUANTITY,
  Details.DELIVERED_QUANTITY,
  Details.PICKED_QUANTITY,
  Details.CREATION_TS,
  Details.LAST_UPDATE_TS,
  Orders.IS_SHIPPED_ON_TIME,
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.IsOnTime`(
    Orders.PROMISE_DATE, Deliveries.CONFIRM_DATE)
    AS IS_DELIVERED_ON_TIME,
  Orders.DAYS_SHIPPED_AFTER_SCHEDULE_DATE,
  DATE_DIFF(Deliveries.CONFIRM_DATE, Orders.PROMISE_DATE, DAY) AS DAYS_DELIVERED_AFTER_PROMISE_DATE
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.Deliveries` AS Deliveries
LEFT JOIN `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.DeliveryAssignments` AS Assignments
  ON Deliveries.DELIVERY_ID = Assignments.DELIVERY_ID
LEFT JOIN `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.DeliveryDetails` AS Details
  ON Assignments.DELIVERY_DETAIL_ID = Details.DELIVERY_DETAIL_ID
LEFT JOIN Orders
  ON
    Details.SOURCE_HEADER_ID = Orders.HEADER_ID
    AND Details.SOURCE_LINE_ID = Orders.LINE_ID
LEFT OUTER JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalDateMD
  ON Deliveries.CONFIRM_DATE = CalDateMD.DATE
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS LedgerMD
  ON Orders.LEDGER_ID = LedgerMD.LEDGER_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.FiscalDateMD` AS FiscalDateMD
  ON
    Deliveries.CONFIRM_DATE = FiscalDateMD.FISCAL_DATE
    AND LedgerMD.PERIOD_SET_NAME = FiscalDateMD.PERIOD_SET_NAME
    AND LedgerMD.PERIOD_TYPE = FiscalDateMD.PERIOD_TYPE
