#--  Copyright 2023 Google Inc.
#
#--  Licensed under the Apache License, Version 2.0 (the "License");
#--  you may not use this file except in compliance with the License.
#--  You may obtain a copy of the License at
#
#--      http://www.apache.org/licenses/LICENSE-2.0
#
#--  Unless required by applicable law or agreed to in writing, software
#--  distributed under the License is distributed on an "AS IS" BASIS,
#--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#--  See the License for the specific language governing permissions and
#--  limitations under the License.

-- Ad Criterions that define targeting
-- (additional conditions _DATA_DATE = DATE_SUB(_LATEST_DATE, INTERVAL 1 DAY)
-- is to account for lacking/ongoing data transfer over the current day)
--

CREATE OR REPLACE VIEW `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_targeting_criterions` AS
(
WITH

--- Part 1 - Ad Group Audiences

-- most recent impressions of ad group audiences
ad_group_audiences_performance AS
(
  SELECT DISTINCT
    ad_group_id,
    _DATA_DATE AS date,
    SUM(metrics_impressions)
      OVER (PARTITION BY ad_group_id, _DATA_DATE)
      AS metrics_impressions
 FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroupAudienceStats_{{ ads_account }}`
 WHERE _DATA_DATE = DATE_SUB(_LATEST_DATE, INTERVAL 1 DAY)
),

-- ad groups performance by date
ad_group_performance AS
(
  SELECT DISTINCT
    ad_group_id,
    _DATA_DATE AS date,
    SUM(metrics_impressions)
      OVER (PARTITION BY ad_group_id, _DATA_DATE)
      AS metrics_impressions
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroupStats_{{ ads_account }}`
),

-- ad groups with performance equal to their combined audiences performance on the most recent date
ad_group_targeted_by_audiences AS
(
  SELECT DISTINCT
    ad_group_audiences_performance.ad_group_id
  FROM ad_group_audiences_performance
  INNER JOIN ad_group_performance
  ON
    ad_group_performance.ad_group_id = ad_group_audiences_performance.ad_group_id
    AND ad_group_performance.date = ad_group_audiences_performance.date
    AND ad_group_performance.metrics_impressions = ad_group_audiences_performance.metrics_impressions
),

--- Part 2 - Campaign Audiences

-- most recent impressions of campaigns with audiences
campaign_audiences_performance AS
(
  SELECT DISTINCT
    crits.campaign_id,
    stats._DATA_DATE AS date,
    SUM(stats.metrics_impressions)
      OVER (PARTITION BY crits.campaign_id, stats._DATA_DATE)
      AS metrics_impressions
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_CampaignAudienceStats_{{ ads_account }}` stats
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_CampaignCriterion_{{ ads_account }}` crits
  ON CAST(stats.campaign_criterion_criterion_id AS INT64) = crits.campaign_criterion_criterion_id
  WHERE
    stats._DATA_DATE = DATE_SUB(stats._LATEST_DATE, INTERVAL 1 DAY)
    AND crits._DATA_DATE = DATE_SUB(crits._LATEST_DATE, INTERVAL 1 DAY)
),

-- campaign performance by date
campaign_performance AS
(
  SELECT DISTINCT
    campaign_id,
    _DATA_DATE AS date,
    SUM(metrics_impressions)
      OVER (PARTITION BY campaign_id, _DATA_DATE)
      AS metrics_impressions
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_CampaignStats_{{ ads_account }}`
),

-- campaigns with performance equal to their combined audiences performance on the most recent date
campaigns_targeted_by_audiences AS
(
  SELECT DISTINCT
    campaign_audiences_performance.campaign_id
  FROM campaign_audiences_performance
  INNER JOIN campaign_performance
  ON
    campaign_performance.campaign_id = campaign_audiences_performance.campaign_id
    AND campaign_performance.date = campaign_audiences_performance.date
    AND campaign_performance.metrics_impressions = campaign_audiences_performance.metrics_impressions
),

--- Part 3 - all ad group criterions from ad groups targeted by audiences

ad_groups_targeting_audiences_recent AS
(
  SELECT DISTINCT
    crit.ad_group_id, crit.ad_group_name,
    crit.ad_group_criterion_criterion_id AS criterion_id,
    crit.ad_group_criterion_display_name AS display_name,
    crit.ad_group_criterion_negative AS is_negative,
    crit.ad_group_criterion_status AS criterion_status,
    FALSE AS is_keyword,
    crit._DATA_DATE AS max_date,
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroupCriterion_{{ ads_account }}` AS crit
  INNER JOIN ad_group_targeted_by_audiences
  ON ad_group_targeted_by_audiences.ad_group_id = crit.ad_group_id
  WHERE
    crit._DATA_DATE = DATE_SUB(crit._LATEST_DATE, INTERVAL 1 DAY)
    AND ad_group_criterion_criterion_id IN
      (SELECT ad_group_criterion_criterion_id
        FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroupCriterion_{{ ads_account }}`
        WHERE CONTAINS_SUBSTR(ad_group_criterion_display_name, "vertical::"))
),

-- make it expanded with campaign_id and campaign_name

ad_groups_targeting_audiences_recent_with_campaigns AS
(
  SELECT DISTINCT
    ad_groups_targeting_audiences_recent.* EXCEPT(max_date),
    ad_group_status, ad_groups.campaign_id, campaign_name,
  FROM ad_groups_targeting_audiences_recent
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroup_{{ ads_account }}` ad_groups
  ON ad_groups.ad_group_id = ad_groups_targeting_audiences_recent.ad_group_id
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_Campaign_{{ ads_account }}` camps
  ON ad_groups.campaign_id = camps.campaign_id
  WHERE
    ad_groups._DATA_DATE = max_date
    AND camps._DATA_DATE = max_date
),

--- Part 4 - all audience criterions from campaigns targeted by audiences

campaign_targeting_audiences_recent AS
(
  SELECT DISTINCT
    crit.campaign_id, crit.campaign_name,
    crit.campaign_criterion_criterion_id AS criterion_id,
    campaign_criterion_display_name AS display_name,
    crit.campaign_criterion_negative AS is_negative,
    crit.campaign_criterion_status AS criterion_status,
    FALSE AS is_keyword,
    crit._DATA_DATE AS max_date,
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_CampaignCriterion_{{ ads_account }}` AS crit
  INNER JOIN campaigns_targeted_by_audiences
  ON campaigns_targeted_by_audiences.campaign_id = crit.campaign_id
  WHERE
    crit._DATA_DATE = DATE_SUB(crit._LATEST_DATE, INTERVAL 1 DAY)
    AND campaign_criterion_criterion_id IN
      (SELECT campaign_criterion_criterion_id FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_CampaignCriterion_{{ ads_account }}`
      WHERE CONTAINS_SUBSTR(campaign_criterion_display_name, "vertical::"))
),


-- make it expended with all ad groups from respective campaigns
campaign_targeting_audiences_recent_with_groups AS
(
  SELECT DISTINCT
    campaign_targeting_audiences_recent.* EXCEPT(max_date),
    ad_group_id,
    ad_group_name,
    ad_group_status,
  FROM campaign_targeting_audiences_recent
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroup_{{ ads_account }}` ad_groups
  ON ad_groups.campaign_id = campaign_targeting_audiences_recent.campaign_id
  WHERE
    ad_groups._DATA_DATE = max_date
),

--- Part 5 - combine all audiences

all_targeting_audiences AS
(
  SELECT
    campaign_id, campaign_name,
    ad_group_id, ad_group_name, ad_group_status,
    criterion_id,
    REGEXP_REPLACE(display_name, r'.*vertical::', '') AS static_criterion_id,
    criterion_status, is_keyword, is_negative
  FROM campaign_targeting_audiences_recent_with_groups
  UNION ALL SELECT
    campaign_id, campaign_name,
    ad_group_id, ad_group_name, ad_group_status,
    criterion_id,
    REGEXP_REPLACE(display_name, r'.*vertical::', '') AS static_criterion_id,
    criterion_status, is_keyword, is_negative
  FROM ad_groups_targeting_audiences_recent_with_campaigns
),

--- Part 6 - convert audience display_name to texts

-- Audience criterions with their actual text values
targeting_audiences_real_texts AS
(
SELECT DISTINCT all_targeting_audiences.* EXCEPT(static_criterion_id),
  COALESCE(affinity.Category_path, categories.Category_path, verticals.Category, products.Category) AS criterion_text
FROM all_targeting_audiences
LEFT JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_affinity_categories` affinity
ON static_criterion_id = CAST(affinity.Category_ID AS STRING)
LEFT JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_in_market_categories` categories
ON static_criterion_id = CAST(categories.Category_ID AS STRING)
LEFT JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_verticals` verticals
ON static_criterion_id = CAST(verticals.Criterion_ID AS STRING)
LEFT JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_products_services` products
ON static_criterion_id = CAST(products.Criterion_ID AS STRING)
),

--- Part 7 - Keywords

-- Keywords
keywords_recent AS
(
  SELECT
    campaign_id, ad_group_id,
    ad_group_criterion_criterion_id AS criterion_id,
    ad_group_criterion_keyword_text AS criterion_text,
    ad_group_criterion_negative AS is_negative,
    ad_group_criterion_status AS criterion_status,
    TRUE AS is_keyword,
    _DATA_DATE AS max_date
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_Keyword_{{ ads_account }}`
  WHERE _DATA_DATE = DATE_SUB(_LATEST_DATE, INTERVAL 1 DAY)
),

-- Keywords with ad group and campaign data

keywords_final AS
(
  SELECT
    keywords_recent.campaign_id, campaign_name,
    keywords_recent.ad_group_id, ad_group_name, ad_group_status,
    criterion_id,
    criterion_status, is_keyword, is_negative,
    criterion_text,
  FROM keywords_recent
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroup_{{ ads_account }}` ad_groups
  ON ad_groups.ad_group_id = keywords_recent.ad_group_id
  AND ad_groups._DATA_DATE = max_date
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_Campaign_{{ ads_account }}` camps
  ON camps.campaign_id = keywords_recent.campaign_id
  AND camps._DATA_DATE = max_date
)

--- Part 8 - Combine everything

SELECT * FROM targeting_audiences_real_texts
WHERE criterion_text IS NOT NULL
UNION ALL
SELECT * FROM keywords_final
WHERE criterion_text IS NOT NULL
ORDER BY campaign_id, ad_group_id, criterion_status, is_keyword, is_negative
)
