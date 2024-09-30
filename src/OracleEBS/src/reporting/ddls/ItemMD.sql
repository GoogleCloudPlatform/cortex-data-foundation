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

-- ItemMD dimension.

WITH
  DistinctItemDescriptions AS (
    SELECT DISTINCT INVENTORY_ITEM_ID, ORGANIZATION_ID, LANGUAGE, DESCRIPTION AS TEXT
    FROM
      `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.MTL_SYSTEM_ITEMS_TL` -- noqa: RF05
    WHERE LANGUAGE IN UNNEST({{ oracle_ebs_languages }})
  ),
  Descriptions AS (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      ARRAY_AGG(STRUCT(LANGUAGE, TEXT)) AS ITEM_DESCRIPTIONS
    FROM
      DistinctItemDescriptions
    GROUP BY INVENTORY_ITEM_ID, ORGANIZATION_ID
  ),
  DistinctCategories AS (
    SELECT DISTINCT
      Categories.INVENTORY_ITEM_ID,
      Categories.ORGANIZATION_ID,
      Categories.CATEGORY_ID AS ID,
      Categories.CATEGORY_SET_ID,
      Categories.CATEGORY_SET_NAME,
      Categories.SEGMENT1 AS CATEGORY_NAME,
      CategoryDescriptions.DESCRIPTION
    FROM
      `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.MTL_ITEM_CATEGORIES` -- noqa: RF05
        AS Categories
    LEFT OUTER JOIN
      `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.MTL_CATEGORIES_B` -- noqa: RF05
        AS CategoryDescriptions
      USING (CATEGORY_ID)
    WHERE Categories.CATEGORY_SET_ID IN UNNEST({{ oracle_ebs_item_category_set_ids }})
  ),
  Categories AS (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      ARRAY_AGG(
        STRUCT(
          ID,
          CATEGORY_SET_ID,
          CATEGORY_SET_NAME,
          CATEGORY_NAME,
          DESCRIPTION)) AS ITEM_CATEGORIES -- noqa: LT02
    FROM
      DistinctCategories
    GROUP BY INVENTORY_ITEM_ID, ORGANIZATION_ID
  )
SELECT
  Item.INVENTORY_ITEM_ID,
  Item.ORGANIZATION_ID,
  Item.ITEM_TYPE,
  Descriptions.ITEM_DESCRIPTIONS,
  Item.SEGMENT1 AS ITEM_PART_NUMBER,
  Item.ITEM_CATALOG_GROUP_ID,
  Categories.ITEM_CATEGORIES,
  Item.SEGMENT2,
  Item.SEGMENT3,
  Item.SEGMENT4,
  Item.ATTRIBUTE_CATEGORY,
  Item.ATTRIBUTE1,
  Item.ATTRIBUTE2,
  Item.ATTRIBUTE3,
  Item.ATTRIBUTE4,
  Item.PURCHASING_ENABLED_FLAG = 'Y' AS IS_PURCHASING_ENABLED,
  Item.CUSTOMER_ORDER_ENABLED_FLAG = 'Y' AS IS_CUSTOMER_ORDER_ENABLED,
  Item.REVISION_QTY_CONTROL_CODE,
  Item.QTY_RCV_EXCEPTION_CODE,
  Item.MARKET_PRICE,
  Item.QTY_RCV_TOLERANCE,
  Item.LIST_PRICE_PER_UNIT,
  Item.UN_NUMBER_ID,
  Item.PRICE_TOLERANCE_PERCENT,
  Item.ROUNDING_FACTOR,
  Item.UNIT_WEIGHT,
  Item.WEIGHT_UOM_CODE,
  Item.VOLUME_UOM_CODE,
  Item.UNIT_VOLUME,
  Item.STD_LOT_SIZE,
  Item.BOM_ITEM_TYPE,
  Item.BASE_ITEM_ID,
  Item.PRIMARY_UOM_CODE,
  Item.PRIMARY_UNIT_OF_MEASURE,
  Item.ALLOWED_UNITS_LOOKUP_CODE,
  Item.COST_OF_SALES_ACCOUNT,
  Item.SALES_ACCOUNT,
  Item.INVENTORY_ITEM_STATUS_CODE,
  Item.MINIMUM_ORDER_QUANTITY,
  Item.FIXED_ORDER_QUANTITY,
  Item.MAXIMUM_ORDER_QUANTITY,
  Item.EAM_ITEM_TYPE,
  Item.APPROVAL_STATUS,
  Item.SUMMARY_FLAG = 'Y' AS IS_SUMMARY,
  Item.ENABLED_FLAG = 'Y' AS IS_ENABLED
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.MTL_SYSTEM_ITEMS_B` AS Item
LEFT OUTER JOIN
  Descriptions
  USING (INVENTORY_ITEM_ID, ORGANIZATION_ID)
LEFT OUTER JOIN
  Categories
  USING (INVENTORY_ITEM_ID, ORGANIZATION_ID)
