CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ k9_datasets_processing }}.holiday_calendar` (
    HolidayDate	STRING,
    Description	STRING,
    CountryCode	STRING,
    Year	STRING,
    WeekDay	STRING,
    QuarterOfYear	INTEGER,
    Week	INTEGER
);
