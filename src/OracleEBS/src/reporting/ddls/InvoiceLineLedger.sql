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

-- InvoiceLineLedger base fact view.

SELECT
  CUST_TRX_LINE_GL_DIST_ID AS INVOICE_LINE_LEGER_ID,
  CUSTOMER_TRX_LINE_ID AS INVOICE_LINE_ID,
  SET_OF_BOOKS_ID AS LEDGER_ID,
  GL_DATE AS LEDGER_DATE,
  CREATION_DATE AS CREATION_TS,
  LAST_UPDATE_DATE AS LAST_UPDATE_TS
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.RA_CUST_TRX_LINE_GL_DIST_ALL`
