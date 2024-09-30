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

-- OrderLines base fact view.

SELECT
  LINE_ID,
  HEADER_ID,
  LINE_NUMBER,
  LINE_CATEGORY_CODE,
  FLOW_STATUS_CODE,
  SHIP_FROM_ORG_ID,
  INVENTORY_ITEM_ID,
  ORDER_QUANTITY_UOM,
  SHIPPED_QUANTITY,
  ORDERED_QUANTITY,
  FULFILLED_QUANTITY,
  INVOICED_QUANTITY,
  CANCELLED_QUANTITY,
  UNIT_COST,
  UNIT_LIST_PRICE,
  UNIT_SELLING_PRICE,
  SCHEDULE_SHIP_DATE,
  ACTUAL_SHIPMENT_DATE AS ACTUAL_SHIP_DATE,
  REQUEST_DATE,
  PROMISE_DATE,
  FULFILLMENT_DATE,
  ACTUAL_FULFILLMENT_DATE,
  BOOKED_FLAG,
  OPEN_FLAG,
  CANCELLED_FLAG,
  FULFILLED_FLAG,
  RETURN_REASON_CODE,
  RETURN_CONTEXT,
  REFERENCE_LINE_ID,
  REFERENCE_HEADER_ID,
  CREATION_DATE AS CREATION_TS,
  LAST_UPDATE_DATE AS LAST_UPDATE_TS
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.OE_ORDER_LINES_ALL`
