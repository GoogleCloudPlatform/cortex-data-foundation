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

-- InvoiceLines base fact view.

SELECT
  CUSTOMER_TRX_LINE_ID AS LINE_ID,
  LINE_NUMBER,
  LINE_TYPE,
  LINK_TO_CUST_TRX_LINE_ID AS LINK_TO_LINE_ID,
  CUSTOMER_TRX_ID AS INVOICE_ID,
  INTERFACE_LINE_CONTEXT,
  INTERFACE_LINE_ATTRIBUTE6,
  DESCRIPTION,
  INVENTORY_ITEM_ID,
  QUANTITY_INVOICED AS INVOICED_QUANTITY,
  QUANTITY_ORDERED AS ORDERED_QUANTITY,
  QUANTITY_CREDITED AS CREDITED_QUANTITY,
  UOM_CODE AS QUANTITY_UOM,
  UNIT_STANDARD_PRICE AS UNIT_LIST_PRICE,
  UNIT_SELLING_PRICE,
  GROSS_UNIT_SELLING_PRICE,
  EXTENDED_AMOUNT AS TRANSACTION_AMOUNT,
  REVENUE_AMOUNT,
  CREATION_DATE AS CREATION_TS,
  LAST_UPDATE_DATE AS LAST_UPDATE_TS
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.RA_CUSTOMER_TRX_LINES_ALL`
