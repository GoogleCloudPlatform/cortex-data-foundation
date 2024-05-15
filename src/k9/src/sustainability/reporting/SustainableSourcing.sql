#-- Copyright 2024 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

SELECT
  lfa1.MANDT AS Client_MANDT,
  lfa1.LIFNR AS VendorAccountNumber_LIFNR,
  esg_score.ORGANIZATION_NAME AS OrganizationName,
  lfa1.KRAUS AS CreditInformationNumber_KRAUS,
  lfa1.LAND1 AS VendorCountry_LAND1,
  PARSE_DATE('%Y%m%d', CAST(esg_score.load_date AS STRING)) AS LoadDate,
  IF(
    LEAD(PARSE_DATE('%Y%m%d', CAST(esg_score.load_date AS STRING)))
      OVER (
        PARTITION BY esg_score.duns
        ORDER BY PARSE_DATE('%Y%m%d', CAST(esg_score.load_date AS STRING)) ASC
      )
    IS NULL,
    CURRENT_DATE(),
    DATE_SUB(
      LEAD(PARSE_DATE('%Y%m%d', CAST(esg_score.load_date AS STRING)))
        OVER (
          PARTITION BY esg_score.duns
          ORDER BY PARSE_DATE('%Y%m%d', CAST(esg_score.load_date AS STRING)) ASC
        ),
      INTERVAL 1 DAY
    )
  ) AS EndDate,
  esg_score.INDUSTRY_SECTOR_CATEGORY AS Industry,
  esg_score.MAJOR_INDUSTRY_CATEGORY AS SectorCategory,
  esg_score.ESG_RANKING_SCORE AS ESGRanking,
  esg_score.ESG_RANKING_AVERAGE_PEER_SCORE AS ESGRankingAveragePeerScore,
  esg_score.ESG_RANKING_PEER_PERCENTILE AS ESGRankingPeerPercentile,
  esg_score.DATA_DEPTH1 AS ESGDataDepth,
  esg_score.ENVIRONMENTAL_RANKING_SCORE AS EnvironmentalRanking,
  esg_score.THEME15_SCORE AS EnergyManagementScore,
  esg_score.THEME24_SCORE AS WaterManagementScore,
  esg_score.THEME21_SCORE AS MaterialSourcingAndManagementScore,
  esg_score.THEME23_SCORE AS WasteAndHazardsManagementScore,
  esg_score.THEME20_SCORE AS LandUseAndBiodiversityScore,
  esg_score.THEME22_SCORE AS PollutionPreventionAndManagementScore,
  esg_score.THEME19_SCORE AS GHGEmissionsScore,
  esg_score.THEME14_SCORE AS ClimateRiskScore,
  esg_score.THEME16_SCORE AS EnvironmentalCertificationsScore,
  esg_score.THEME17_SCORE AS EnvironmentalComplianceScore,
  esg_score.THEME18_SCORE AS EnvironmentalOpportunitiesScore,
  esg_score.SOCIAL_RANKING_SCORE AS SocialRanking,
  esg_score.THEME29_SCORE AS DiversityAndInclusionScore,
  esg_score.THEME30_SCORE AS HealthAndSafetyScore,
  esg_score.THEME31_SCORE AS HumanRightAbusesScore,
  esg_score.THEME32_SCORE AS LaborRelationsScore,
  esg_score.THEME37_SCORE AS TrainingAndEducationScore,
  esg_score.THEME27_SCORE AS CyberRiskScore,
  esg_score.THEME34_SCORE AS ProductManagementAndQualityScore,
  esg_score.THEME33_SCORE AS ProductAndServicesScore,
  esg_score.THEME28_SCORE AS DataPrivacyScore,
  esg_score.THEME26_SCORE AS CorporatePhilanthropyScore,
  esg_score.THEME25_SCORE AS CommunityEngagementScore,
  esg_score.THEME36_SCORE AS SupplierEngagementScore,
  esg_score.THEME35_SCORE AS SocialRelatedCertificatesScore,
  esg_score.GOVERNANCE_RANKING_SCORE AS GovernanceRanking,
  esg_score.THEME39_SCORE AS BusinessEthicsScore,
  esg_score.THEME38_SCORE AS BoardAccountabilityScore,
  -- The following scores has type STRING in the data dictionary
  -- but denotes scores between 0-100 both by definition and from
  -- sample data. Hence the casting to INT.
  SAFE_CAST(esg_score.VAR1 AS INT64) AS BusinessTransparencyScore,
  SAFE_CAST(esg_score.VAR2 AS INT64) AS CorporateComplianceBehaviorsScore,
  SAFE_CAST(esg_score.VAR3 AS INT64) AS GovernanceRelatedCertificationsScore,
  SAFE_CAST(esg_score.VAR4 AS INT64) AS ShareholderRightsScore,
  esg_score.THEME40_SCORE AS BusinessResilienceAndSustainabilityScore
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.lfa1` AS lfa1
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.dun_bradstreet_esg` AS esg_score
  ON
    CAST(LFA1.KRAUS AS INT64) = esg_score.DUNS
WHERE lfa1.MANDT = '{{ mandt }}'
