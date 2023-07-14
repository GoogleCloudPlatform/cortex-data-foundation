CREATE TABLE IF NOT EXISTS `{{ project_id_src  }}.{{ k9_datasets_processing }}.trends` (
    WeekStart DATE,
    InterestOverTime	INT64,
    CountryCode	STRING,
    HierarchyId	STRING,
    HierarchyText	STRING
);

