CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement` (
  client STRING,
  companycode STRING,
  businessarea STRING,
  ledger STRING,
  profitcenter STRING,
  costcenter STRING,
  glaccount STRING,
  fiscalyear STRING,
  fiscalperiod STRING,
  fiscalquarter INT64,
  --noqa: disable=L008
  {% if sql_flavour == 'ecc' -%}
  balancesheetaccountindicator STRING,
  placcountindicator STRING,
  {% else %}
  balancesheetandplaccountindicator STRING,
  {% endif -%}
  --noqa: enable=all
  amount NUMERIC,
  currency STRING,
  companytext STRING,
  sequence INT
);

CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement`(
  input_startdate DATE, input_enddate DATE)
BEGIN
  --This procedure creates table having transaction data at fiscal year, period level
  --including copying missing records from one period to another.
  DECLARE sequence_length INT64 DEFAULT NULL;
  DECLARE fiscal_iteration INT64 DEFAULT 1;
  DECLARE company_array ARRAY <STRING(15)>;
  DECLARE company_length INT64 DEFAULT 1;
  DECLARE company_iteration INT64 DEFAULT 0;
  DECLARE updated_startdate DATE DEFAULT NULL;

  UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  SET sequence = NULL
  WHERE TRUE;

  SET updated_startdate = (
    WITH PrevFiscalYearPeriod AS (
        SELECT MAX(PrevPeriod) AS PrevPeriod FROM
          (
          SELECT
            Starting_Date,
            LAG(FiscalYearPeriod) OVER (ORDER BY FiscalYearPeriod) AS PrevPeriod
          FROM
          (
            SELECT DISTINCT FiscalYearPeriod, MIN(Date) AS Starting_Date
            FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim`
            WHERE mandt = '{{ mandt }}'
            GROUP BY FiscalYearPeriod
          )
        )
        WHERE Starting_Date <= input_startdate
    ),
    PreviousPeriodDiscovered AS
    (
      SELECT MIN(Date) AS Date
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim`
      WHERE FiscalYearPeriod IN
      (
        SELECT PrevPeriod FROM PrevFiscalYearPeriod
      )
    )
    SELECT Date
    FROM PreviousPeriodDiscovered
  );

  {% if sql_flavour == 'ecc' -%}
    CREATE OR REPLACE TEMP TABLE AccountingDocuments AS (
      SELECT
        bkpf.MANDT,
        faglflexa.RBUKRS,
        faglflexa.RLDNR,
        faglflexa.RBUSA,
        faglflexa.RCNTR,
        faglflexa.RACCT,
        faglflexa.PRCTR,
        FiscalDateDimension.FiscalYear,
        FiscalDateDimension.FiscalPeriod,
        MAX(FiscalDateDimension.FiscalQuarter) AS FiscalQuarter,
        MAX(bseg.XBILK) AS XBILK,
        MAX(bseg.GVTYP) AS GVTYP,
        MAX(t001.PERIV) AS PERIV,
        MAX(t001.BUTXT) AS BUTXT,
        MAX(faglflexa.RWCUR) AS RWCUR,
        SUM(COALESCE(faglflexa.HSL * currency_decimal.CURRFIX, faglflexa.HSL)) AS HSL
      FROM
        (
          SELECT
            MANDT,
            BUKRS,
            GJAHR,
            BELNR,
            BUZEI,
            XBILK,
            GVTYP
          FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.bseg`
          WHERE mandt = '{{ mandt }}'
        ) AS bseg
      INNER JOIN -- noqa: disable=L042
        (
          SELECT
            MANDT,
            BUKRS,
            GJAHR,
            BELNR
          FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.bkpf`
          WHERE mandt = '{{ mandt }}'
            --Ignoring the reversal documents
            AND XREVERSAL IS NULL
        ) AS bkpf
        ON
          bkpf.MANDT = bseg.MANDT
          AND bkpf.BUKRS = bseg.BUKRS
          AND bkpf.GJAHR = bseg.GJAHR
          AND bkpf.BELNR = bseg.BELNR
      INNER JOIN
        (
          SELECT
            RCLNT,
            RBUKRS,
            RYEAR,
            BELNR,
            BUZEI,
            RWCUR,
            BUDAT,
            RLDNR,
            RBUSA,
            RCNTR,
            RACCT,
            PRCTR,
            HSL
          FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.faglflexa`
          WHERE rclnt = '{{ mandt }}'
            AND budat BETWEEN input_startdate AND input_enddate
        ) AS faglflexa -- noqa: enable=all
        ON
          bkpf.MANDT = faglflexa.RCLNT
          AND bkpf.BUKRS = faglflexa.RBUKRS
          AND bkpf.GJAHR = faglflexa.RYEAR
          AND bkpf.BELNR = faglflexa.BELNR
          AND bseg.BUZEI = faglflexa.BUZEI
      LEFT JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
        ON
          bseg.MANDT = t001.MANDT
          AND faglflexa.RBUKRS = t001.BUKRS
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
        ON faglflexa.RWCUR = currency_decimal.CURRKEY
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
        ON
          bseg.MANDT = FiscalDateDimension.MANDT
          AND t001.PERIV = FiscalDateDimension.PERIV
          AND faglflexa.BUDAT = FiscalDateDimension.DATE
      GROUP BY
        bkpf.MANDT, faglflexa.RBUKRS, faglflexa.RLDNR, faglflexa.RBUSA, faglflexa.RCNTR,
        faglflexa.RACCT, faglflexa.PRCTR, FiscalDateDimension.FiscalYear,
        FiscalDateDimension.FiscalPeriod
    );

    CREATE OR REPLACE TEMP TABLE FiscalDimension AS (
      SELECT DISTINCT
        AccountingDocuments.MANDT,
        AccountingDocuments.RBUKRS,
        FiscalDateDimension.FiscalYear,
        FiscalDateDimension.FiscalPeriod,
        FiscalDateDimension.FiscalQuarter,
        FiscalDateDimension.PERIV,
        DENSE_RANK() OVER (
          PARTITION BY AccountingDocuments.RBUKRS
          ORDER BY FiscalDateDimension.FiscalYear ASC,
            FiscalDateDimension.FiscalPeriod ASC) AS sequence
      FROM
        (
          SELECT DISTINCT MANDT, RBUKRS, PERIV
          FROM AccountingDocuments
        ) AS AccountingDocuments
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
        ON
          AccountingDocuments.MANDT = FiscalDateDimension.MANDT
          AND AccountingDocuments.PERIV = FiscalDateDimension.PERIV
      WHERE
        FiscalDateDimension.DATE
        BETWEEN
        -- Subtracting one more month from input_date to have an additional month at the beginning
        -- in the FiscalDimension as it is needed for the periodical load
          updated_startdate
        AND
          input_enddate -- noqa: L027
    );

    DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    WHERE
      (FiscalYear, FiscalPeriod) IN
      (
        SELECT (FiscalYear, FiscalPeriod)
        FROM FiscalDimension
        -- Eliminate the lowest available period as we've selected it additionally for the
        -- FiscalDateDimension table only
        WHERE sequence <> 1
      );

    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
     fiscalyear, fiscalperiod, fiscalquarter, balancesheetaccountindicator, placcountindicator,
     amount, currency, companytext, sequence)
    SELECT
      FiscalDimension.MANDT,
      FiscalDimension.RBUKRS,
      AccountingDocuments.RBUSA,
      AccountingDocuments.RLDNR,
      AccountingDocuments.PRCTR,
      AccountingDocuments.RCNTR,
      AccountingDocuments.RACCT,
      FiscalDimension.FiscalYear,
      FiscalDimension.FiscalPeriod,
      FiscalDimension.FiscalQuarter,
      AccountingDocuments.XBILK,
      AccountingDocuments.GVTYP,
      COALESCE(AccountingDocuments.HSL, 0),
      AccountingDocuments.RWCUR,
      AccountingDocuments.BUTXT,
      FiscalDimension.sequence
    FROM FiscalDimension
    LEFT JOIN AccountingDocuments
      ON
        FiscalDimension.MANDT = AccountingDocuments.MANDT
        AND FiscalDimension.RBUKRS = AccountingDocuments.RBUKRS
        AND FiscalDimension.FiscalYear = AccountingDocuments.FiscalYear
        AND FiscalDimension.FiscalPeriod = AccountingDocuments.FiscalPeriod
    WHERE sequence > 1;

  SET company_array = ARRAY(SELECT DISTINCT RBUKRS FROM FiscalDimension ORDER BY RBUKRS);

  {% else -%}
  CREATE OR REPLACE TEMP TABLE AccountingDocuments AS (
    SELECT
      acdoca.RCLNT,
      acdoca.RBUKRS,
      acdoca.RLDNR,
      acdoca.RBUSA,
      acdoca.RCNTR,
      acdoca.RACCT,
      acdoca.PRCTR,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      MAX(FiscalDateDimension.FiscalQuarter) AS FiscalQuarter,
      MAX(acdoca.GLACCOUNT_TYPE) AS GLACCOUNT_TYPE,
      MAX(t001.PERIV) AS PERIV,
      MAX(t001.BUTXT) AS BUTXT,
      MAX(acdoca.RHCUR) AS RHCUR,
      SUM(COALESCE(acdoca.HSL * currency_decimal.CURRFIX, acdoca.HSL)) AS HSL
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.acdoca` AS acdoca
    LEFT JOIN
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
      ON acdoca.RCLNT = t001.MANDT AND acdoca.RBUKRS = t001.BUKRS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
      ON acdoca.RHCUR = currency_decimal.CURRKEY
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
      ON
        acdoca.RCLNT = FiscalDateDimension.MANDT
        AND t001.PERIV = FiscalDateDimension.PERIV
        AND acdoca.BUDAT = FiscalDateDimension.DATE
    WHERE
      acdoca.RCLNT = '{{ mandt }}'
      --Ignoring the reversal documents
      AND acdoca.XTRUEREV IS NULL
      --- Ensuring we only get records for PL and BalanceSheet
      AND acdoca.GLACCOUNT_TYPE IN ('X','P','N')
    GROUP BY RCLNT,RBUKRS,RLDNR,RBUSA,RCNTR,RACCT,PRCTR,FiscalYear,FiscalPeriod
  );

  CREATE OR REPLACE TEMP TABLE FiscalDimension
  AS (
    SELECT DISTINCT
      AccountingDocuments.RCLNT,
      AccountingDocuments.RBUKRS,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      FiscalDateDimension.FiscalQuarter,
      FiscalDateDimension.PERIV,
      DENSE_RANK() OVER (
        PARTITION BY AccountingDocuments.RBUKRS
        ORDER BY  FiscalDateDimension.FiscalYear ASC,
          FiscalDateDimension.FiscalPeriod ASC) AS sequence
    FROM (
        SELECT DISTINCT
          RCLNT,
          RBUKRS,
          PERIV
        FROM AccountingDocuments
    ) AS AccountingDocuments
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
      ON
        AccountingDocuments.RCLNT = FiscalDateDimension.MANDT
        AND AccountingDocuments.PERIV = FiscalDateDimension.PERIV
    WHERE
      FiscalDateDimension.DATE
      BETWEEN
        -- Subtracting one more month from input_date to have an additional month at the beginning
        -- in the FiscalDimension as it is needed for the periodical load
        updated_startdate
      AND
        input_enddate -- noqa: L027
  );

  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  WHERE
    (FiscalYear, FiscalPeriod) IN
    (
      SELECT (FiscalYear, FiscalPeriod)
      FROM FiscalDimension
      -- Eliminate the lowest available period as we've selected it additionally for the
      -- FiscalDateDimension table only
      WHERE sequence <> 1
    );

  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
   fiscalyear, fiscalperiod, fiscalquarter, balancesheetandplaccountindicator, amount, currency,
   companytext, sequence)
  SELECT
    FiscalDimension.RCLNT,
    FiscalDimension.RBUKRS,
    AccountingDocuments.RBUSA,
    AccountingDocuments.RLDNR,
    AccountingDocuments.PRCTR,
    AccountingDocuments.RCNTR,
    AccountingDocuments.RACCT,
    FiscalDimension.FiscalYear,
    FiscalDimension.FiscalPeriod,
    FiscalDimension.FiscalQuarter,
    AccountingDocuments.GLACCOUNT_TYPE,
    COALESCE(AccountingDocuments.HSL, 0),
    AccountingDocuments.RHCUR,
    AccountingDocuments.BUTXT,
    FiscalDimension.sequence
  FROM FiscalDimension
  LEFT JOIN AccountingDocuments
    ON
      FiscalDimension.RCLNT = AccountingDocuments.RCLNT
      AND FiscalDimension.RBUKRS = AccountingDocuments.RBUKRS
      AND FiscalDimension.FiscalYear = AccountingDocuments.FiscalYear
      AND FiscalDimension.FiscalPeriod = AccountingDocuments.FiscalPeriod
  WHERE sequence > 1;

  SET company_array = ARRAY(SELECT DISTINCT RBUKRS FROM FiscalDimension ORDER BY RBUKRS);

  {% endif -%}
  SET company_length = ARRAY_LENGTH(company_array);

  WHILE(company_iteration < company_length) DO

    UPDATE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    SET sequence =
      (
        SELECT DISTINCT MIN(sequence)-1
        FROM  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
        WHERE companycode = company_array[company_iteration]
      )
    WHERE CONCAT(FiscalYear,FiscalPeriod) IN
      (
        SELECT DISTINCT FiscalYearperiod
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDim
        INNER JOIN
        (
          SELECT PERIV
          FROM FiscalDimension
          WHERE RBUKRS = company_array[company_iteration]
        ) AS FiscalDimension
        ON FiscalDateDim.PERIV = FiscalDimension.PERIV
        WHERE DATE = updated_startdate
      )
    AND companycode = company_array[company_iteration]
    AND sequence IS NULL;

    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
      (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
      fiscalyear, fiscalperiod, fiscalquarter,
      --noqa: disable=L008
      {% if sql_flavour == 'ecc' -%}
      balancesheetaccountindicator,
      placcountindicator,
      {% else -%}
      balancesheetandplaccountindicator,
      {% endif -%}
      amount, currency, companytext, sequence)
      --noqa: enable=all
    SELECT *
    FROM
    (
      SELECT
        client,
        companycode,
        businessarea,
        ledger,
        profitcenter,
        costcenter,
        glaccount,
        lkp.FiscalYear AS fiscalyear,
        lkp.FiscalPeriod AS fiscalperiod,
        lkp.FiscalQuarter AS fiscalquarter,
        {% if sql_flavour == 'ecc' -%}
        balancesheetaccountindicator,
        placcountindicator,
        {% else -%}
        balancesheetandplaccountindicator,
        {% endif -%}
        amount,
        currency,
        companytext,
        resultant_rec.sequence
      FROM
      (
        SELECT
          PreviousPeriod.client,
          PreviousPeriod.companycode,
          PreviousPeriod.businessarea,
          PreviousPeriod.ledger,
          PreviousPeriod.profitcenter,
          PreviousPeriod.costcenter,
          PreviousPeriod.glaccount,
          CAST(PreviousPeriod.fiscalyear AS INT64) AS fiscalyear,
          CAST(PreviousPeriod.fiscalperiod AS INT64) AS fiscalperiod,
          PreviousPeriod.fiscalquarter,
          {% if sql_flavour == 'ecc' -%}
          PreviousPeriod.balancesheetaccountindicator,
          PreviousPeriod.placcountindicator,
          {% else -%}
          PreviousPeriod.balancesheetandplaccountindicator,
          {% endif -%}
          0 AS amount,
          PreviousPeriod.currency,
          PreviousPeriod.companytext,
          PreviousPeriod.sequence+1 AS sequence,
          CONCAT(PreviousPeriod.glaccount, COALESCE(PreviousPeriod.profitcenter, ''),
            PreviousPeriod.companycode,
            COALESCE(PreviousPeriod.businessarea, ''), COALESCE(PreviousPeriod.costcenter, ''),
            COALESCE(PreviousPeriod.ledger,'')) AS uniquePrevcombination,
          CONCAT(CurrentPeriod.glaccount, COALESCE(CurrentPeriod.profitcenter, ''),
            CurrentPeriod.companycode,
            COALESCE(CurrentPeriod.businessarea,''), COALESCE(CurrentPeriod.costcenter, ''),
            COALESCE(CurrentPeriod.ledger, '')) AS uniqueCurrcombination
        FROM
        (
          WITH EXISTING_PERIODS AS
          (
            WITH
              EXISTING_TRANSACTIONS AS
              (
                SELECT DISTINCT
                  fiscalperiod, fiscalyear, client, companycode, businessarea, ledger,
                  profitcenter, costcenter, glaccount,
                  {% if sql_flavour == 'ecc' -%}
                  balancesheetaccountindicator,
                  placcountindicator,
                  {% else -%}
                  balancesheetandplaccountindicator,
                  {% endif -%}
                  amount, currency, companytext
                FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
                WHERE companycode = company_array[company_iteration]
              ),
              ALL_PERIODS AS
              (
                SELECT FiscalDimension.* FROM FiscalDimension
                INNER JOIN
                  -- Exclude the transactions having no records for a period for a company
                  (
                    SELECT DISTINCT companycode, fiscalperiod, fiscalyear
                    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
                  ) AS FinStatement
                  ON FiscalDimension.RBUKRS = FinStatement.companycode
                  AND FiscalDimension.FiscalYear = FinStatement.fiscalyear
                  AND FiscalDimension.FiscalPeriod = FinStatement.fiscalperiod
                WHERE FiscalDimension.RBUKRS = company_array[company_iteration]
              )
              SELECT
                ALL_PERIODS.sequence, ALL_PERIODS.FiscalYear, ALL_PERIODS.FiscalPeriod,
                ALL_PERIODS.FiscalQuarter, EXISTING_TRANSACTIONS.client,
                EXISTING_TRANSACTIONS.companycode, EXISTING_TRANSACTIONS.businessarea,
                EXISTING_TRANSACTIONS.ledger, EXISTING_TRANSACTIONS.profitcenter,
                EXISTING_TRANSACTIONS.costcenter, EXISTING_TRANSACTIONS.glaccount,
                {% if sql_flavour == 'ecc' -%}
                EXISTING_TRANSACTIONS.balancesheetaccountindicator,
                EXISTING_TRANSACTIONS.placcountindicator,
                {% else -%}
                EXISTING_TRANSACTIONS.balancesheetandplaccountindicator,
                {% endif -%}
                EXISTING_TRANSACTIONS.amount,
                EXISTING_TRANSACTIONS.currency, EXISTING_TRANSACTIONS.companytext
              FROM EXISTING_TRANSACTIONS
              INNER JOIN ALL_PERIODS
                ON EXISTING_TRANSACTIONS.companycode = ALL_PERIODS.RBUKRS
                AND EXISTING_TRANSACTIONS.FiscalYear = ALL_PERIODS.fiscalyear
                AND EXISTING_TRANSACTIONS.FiscalPeriod = ALL_PERIODS.fiscalperiod
          ),
          MAX_PERIOD AS
          (
            WITH
              EXISTING_TRANSACTIONS AS
              (
                -- Rank all the records to find out the maximum period in the existing data
                SELECT * FROM
                (
                  SELECT client, companycode, businessarea, ledger, profitcenter, costcenter,
                    glaccount,
                    {% if sql_flavour == 'ecc' -%}
                    balancesheetaccountindicator,
                    placcountindicator,
                    {% else -%}
                    balancesheetandplaccountindicator,
                    {% endif -%}
                    currency,
                    companytext, amount, fiscalyear, fiscalperiod,
                    RANK()
                      OVER (
                        PARTITION BY
                          glaccount,
                          businessarea,
                          companycode,
                          profitcenter,
                          costcenter,
                          ledger
                        ORDER BY fiscalyear DESC, fiscalperiod DESC
                      ) AS LATEST_PERIOD
                  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
                  WHERE companycode=company_array[company_iteration]
                  GROUP BY
                    client,
                    companycode,
                    businessarea,
                    ledger,
                    profitcenter,
                    costcenter,
                    glaccount,
                    {% if sql_flavour == 'ecc' -%}
                    balancesheetaccountindicator,
                    placcountindicator,
                    {% else -%}
                    balancesheetandplaccountindicator,
                    {% endif -%}
                    currency,
                    companytext, amount, fiscalyear, fiscalperiod
                )
                WHERE LATEST_PERIOD = 1
              ),
              ALL_PERIODS AS
              (
                -- Find the maximum period as per the FinStatement table
                SELECT * FROM
                (
                  SELECT FinDimension.* ,
                  RANK()
                    OVER (
                      PARTITION BY RBUKRS
                    ORDER BY sequence DESC
                    ) AS LATEST_PERIOD
                  FROM FiscalDimension AS FinDimension
                  INNER JOIN
                  (
                    SELECT DISTINCT companycode, fiscalperiod, fiscalyear
                    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
                  ) AS FinStatement
                    ON FinDimension.RBUKRS = FinStatement.companycode
                    AND FinDimension.FiscalYear = FinStatement.fiscalyear
                    AND FinDimension.FiscalPeriod = FinStatement.fiscalperiod
                  WHERE FinDimension.RBUKRS = company_array[company_iteration]
                  ORDER BY FinDimension.FiscalYear, FinDimension.FiscalPeriod
                )
                WHERE LATEST_PERIOD = 1
              )
              SELECT
                -- Select the record for maximum period possible as per company's other transations
                ALL_PERIODS.sequence, ALL_PERIODS.FiscalYear, ALL_PERIODS.FiscalPeriod,
                ALL_PERIODS.FiscalQuarter, EXISTING_TRANSACTIONS.client,
                EXISTING_TRANSACTIONS.companycode, EXISTING_TRANSACTIONS.businessarea,
                EXISTING_TRANSACTIONS.ledger, EXISTING_TRANSACTIONS.profitcenter,
                EXISTING_TRANSACTIONS.costcenter, EXISTING_TRANSACTIONS.glaccount,
                {% if sql_flavour == 'ecc' -%}
                EXISTING_TRANSACTIONS.balancesheetaccountindicator,
                EXISTING_TRANSACTIONS.placcountindicator,
                {% else -%}
                EXISTING_TRANSACTIONS.balancesheetandplaccountindicator,
                {% endif -%}
                -- If the latest period is same as the max period, use original amount so the
                -- record gets eliminated in a Union Distinct operation, else use zero
                IF(ALL_PERIODS.FiscalYear = EXISTING_TRANSACTIONS.fiscalyear AND
                    ALL_PERIODS.FiscalPeriod = EXISTING_TRANSACTIONS.FiscalPeriod,
                    EXISTING_TRANSACTIONS.amount, 0
                  ) AS amount,
                EXISTING_TRANSACTIONS.currency, EXISTING_TRANSACTIONS.companytext
              FROM EXISTING_TRANSACTIONS
              CROSS JOIN ALL_PERIODS
          ),
          MERGED_PERIODS AS (
            SELECT * FROM EXISTING_PERIODS
            UNION DISTINCT
            SELECT * FROM MAX_PERIOD
          ),
          GEN_REC AS (
            SELECT
            COALESCE(
              LEAD(sequence)
                OVER (
                  PARTITION BY
                    glaccount,
                    profitcenter,
                    companycode,
                    businessarea,
                    costcenter,
                    ledger
                  ORDER BY sequence
                ) - sequence -1, 0
            ) AS missing_periods, *
            FROM MERGED_PERIODS
          ),
          MASTER_SEQ AS (
            SELECT * FROM FiscalDimension WHERE
            RBUKRS=company_array[company_iteration]
          ),
          FABRICATED_RECORDS AS (
            SELECT
              -- Populate the column values of newly generated records like
              -- fiscalyear, fiscalperiod and zero amount values
              GEN_REC.* EXCEPT(sequence, fiscalyear, fiscalperiod, fiscalquarter, amount),
              IF (MASTER_SEQ.sequence - GEN_REC.sequence = 0, GEN_REC.amount, 0) AS amount,
              MASTER_SEQ.FiscalYear AS fiscalyear,
              MASTER_SEQ.FiscalPeriod AS fiscalperiod,
              MASTER_SEQ.FiscalQuarter AS fiscalquarter,
              MASTER_SEQ.sequence AS sequence
            FROM GEN_REC
            INNER JOIN MASTER_SEQ
            ON
              --Join for the range of missing records to create rows of missing records
              MASTER_SEQ.sequence BETWEEN
              GEN_REC.sequence AND (GEN_REC.sequence + GEN_REC.missing_periods)
          )
          SELECT * FROM FABRICATED_RECORDS
          WHERE
            -- Eliminate the records with unique combination null
            CONCAT(glaccount, COALESCE(profitcenter,''), companycode,
            COALESCE(businessarea,''), COALESCE(costcenter,''),
            COALESCE(ledger,'')) IS NOT NULL
        ) AS PreviousPeriod
        LEFT JOIN
        (
          -- Compare the succeeding and preceding months to determine if the combination exists
          -- in the succeeding month
          SELECT * FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
          WHERE companycode = company_array[company_iteration]
        ) AS CurrentPeriod
        ON
          PreviousPeriod.sequence = CurrentPeriod.sequence - 1 AND
          CONCAT(PreviousPeriod.glaccount, COALESCE(PreviousPeriod.profitcenter,''),
            PreviousPeriod.companycode,
            COALESCE(PreviousPeriod.businessarea,''), COALESCE(PreviousPeriod.costcenter,''),
            COALESCE(PreviousPeriod.ledger,'')) =
          CONCAT(CurrentPeriod.glaccount, COALESCE(CurrentPeriod.profitcenter,''),
            CurrentPeriod.companycode,
            COALESCE(CurrentPeriod.businessarea,''), COALESCE(CurrentPeriod.costcenter,''),
            COALESCE(CurrentPeriod.ledger,''))
      ) AS resultant_rec
      LEFT JOIN
      (
        -- Lookup the sequence of final selected records in FiscalDimension to eliminate
        -- 1. The sequences not in FiscalDimension (these are borderline cases having last month)
        -- 2. Eliminate the records where uniqueCurrcombination is null, this would have been set
        --    in the last left join
        -- 3. Eliminate the records where uniquePrevcombination is not null, this again is set as a
        --    result of last left join
        SELECT * FROM FiscalDimension
        WHERE
          RBUKRS=company_array[company_iteration]
      ) AS lkp
      ON resultant_rec.sequence = lkp.sequence
      WHERE
        uniqueCurrcombination IS NULL AND
        uniquePrevcombination IS NOT NULL AND
        resultant_rec.sequence IS NOT NULL
    )
    WHERE
      fiscalperiod IS NOT NULL;

    DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    WHERE (fiscalyear, fiscalperiod) NOT IN
    (
      SELECT (fiscalyear, fiscalperiod)
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
      WHERE companycode = company_array[company_iteration] AND amount <> 0
      GROUP BY fiscalyear, fiscalperiod
    )
    AND companycode = company_array[company_iteration];

    SET fiscal_iteration = 1;
    SET company_iteration = company_iteration + 1;
  END WHILE;

  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  WHERE glaccount IS NULL;

END;
