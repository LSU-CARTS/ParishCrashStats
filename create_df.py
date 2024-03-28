import pandas as pd


def create_licensed_driver_df(engine, start_year, end_year):
    sql_query = f"""
        SELECT CAST([Year] AS INT) AS 'YEAR',
          SUM(LicensedDrivers) AS 'LICENSED DRIVER POPULATION',
          SUM(YouthLicensedDrivers) AS 'LICENSED DRIVER POPULATION OF AGES 15-24',
          CONCAT(CAST(SUM(YouthLicensedDrivers) * 100. / SUM(LicensedDrivers) AS DECIMAL(10,2)), '%') 
            AS 'PERCENT OF 15-24 YEAR OLD DRIVERS',
          FPND.ParishCode, 
          RP.ParishDescription
        FROM FactParishNormalizedData FPND
        INNER JOIN RefParish RP ON FPND.ParishCode = RP.ParishCode
        WHERE [Year] BETWEEN {start_year} AND {end_year}
        GROUP BY FPND.ParishCode, RP.ParishDescription, [Year]
    """

    return pd.read_sql_query(sql_query, engine)


def create_total_crash_df(engine, start_year, end_year):
    sql_query = f"""
        WITH FatalCrashes AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(CrashSeverityCode) AS 'NUMBER OF X CRASHES', 
                SUM(CAST(YoungDriver AS smallint)) AS 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                CONCAT(CAST(SUM(CAST(YoungDriver AS smallint)) * 100./COUNT(CrashSeverityCode) AS DECIMAL(10, 2)), '%') 
                  AS 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                ParishCode, 
                Parish,
                'Fatal' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            GROUP BY ParishCode, Parish, [CrashYear]
        ),
        InjuryCrashes AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(CrashSeverityCode) AS 'NUMBER OF X CRASHES', 
                SUM(CAST(YoungDriver AS smallint)) AS 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                CONCAT(CAST(SUM(CAST(YoungDriver AS smallint)) * 100./COUNT(CrashSeverityCode) AS DECIMAL(10, 2)), '%') 
                  AS 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                ParishCode, 
                Parish,
                'Injury' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            GROUP BY ParishCode, Parish, [CrashYear]
        ),
        PDOCrashes AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(CrashSeverityCode) AS 'NUMBER OF X CRASHES', 
                SUM(CAST(YoungDriver AS smallint)) AS 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                CONCAT(CAST(SUM(CAST(YoungDriver AS smallint)) * 100./COUNT(CrashSeverityCode) AS DECIMAL(10, 2)), '%') 
                  AS 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                ParishCode, 
                Parish,
                'PDO' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 104 -- (O) PROPERTY DAMAGE ONLY
            AND ParishCode <> -1
            GROUP BY ParishCode, Parish, [CrashYear]
        )
        SELECT * FROM FatalCrashes
        UNION ALL
        SELECT * FROM InjuryCrashes
        UNION ALL
        SELECT * FROM PDOCrashes
        ORDER BY 'CrashType', Parish ASC, [YEAR] ASC
    """

    return pd.read_sql_query(sql_query, engine)


def create_fat_and_injury_df(engine, start_year, end_year):
    sql_query = f"""
        WITH FatalitiesAndInjuries AS (
            SELECT 
                FP.CrashYear AS [YEAR],
                ParishCode, 
                Parish,
                CASE 
                    WHEN InjuryStatusCode = 100 THEN 'Fatalities'
                    ELSE 'Injuries'
                END AS InjuryStatus,
                COUNT(CASE WHEN InjuryStatusCode = 100 THEN 1 END) AS NumberOfFatalities,
                COUNT(CASE WHEN InjuryStatusCode IN (101, 102, 103) THEN 1 END) AS NumberOfInjuries
            FROM FactPerson FP
            INNER JOIN FactCrash FC ON FP.CrashSK = FC.CrashPK
            WHERE FP.CrashYear BETWEEN {start_year} AND {end_year}
            GROUP BY FP.CrashYear, ParishCode, Parish,
                CASE 
                    WHEN InjuryStatusCode = 100 THEN 'Fatalities'
                    ELSE 'Injuries'
                END
        )
        SELECT [YEAR], ParishCode, Parish, InjuryStatus,
            CASE 
                WHEN InjuryStatus = 'Fatalities' THEN NumberOfFatalities
                ELSE NumberOfInjuries
            END AS [NUMBER OF X]
        FROM FatalitiesAndInjuries
        ORDER BY InjuryStatus, Parish ASC, [YEAR] ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_safety_belt_df(engine, start_year, end_year):
    sql_query = f"""
        WITH CrashesAndPersons AS (
            SELECT fc.CrashYear AS [YEAR], ParishCode, 
              COUNT(DISTINCT(CASE WHEN fp.NonRestraintPerson = 1 THEN fp.PERSONPK END)) AS UNRESTRAINED_DRIVERS_KILLED, 
              COUNT(DISTINCT(fp.PERSONPK)) AS DRIVERS_KILLED
            FROM [dbo].[FactCrash] fc
            INNER JOIN [dbo].[FactPerson] fp ON fc.[CrashPK] = fp.[CrashSK]
            WHERE fc.[CrashYear] BETWEEN {start_year} AND {end_year}
            AND fp.[PersonTypeCode] = '001'
            AND fp.[InjuryStatusCode] = '100'
            GROUP BY fc.CrashYear, ParishCode
        ),
        PARISH AS (
            SELECT ParishCode, ParishDescription
            FROM RefParish
        )
        SELECT CAP.[YEAR] AS [YEAR],
            CONCAT(
                CAST(SUM(CAP.UNRESTRAINED_DRIVERS_KILLED) * 100. / SUM(CAP.DRIVERS_KILLED) AS DECIMAL(10, 2)),
                '%'
            ) AS 'PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT',
            p.ParishDescription AS 'Parish'
        FROM CrashesAndPersons CAP
        LEFT JOIN PARISH p ON CAP.ParishCode = p.ParishCode
        GROUP BY p.ParishDescription, CAP.[YEAR]
        ORDER BY p.ParishDescription ASC, CAP.[YEAR] ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_cost_estimate_df(engine, start_year, end_year):
    sql_query = f"""
        WITH INJURIES AS (
            SELECT COUNT(DISTINCT(VEHICLESK)) AS 'VEHICLES',
              C.CrashYear AS 'YEAR', 
              C.ParishCode AS 'PARISH',
              convert(float, SUM(CASE WHEN InjuryStatusCode = '100' THEN 1 ELSE 0 END)) AS 'FATALATIES',
              convert(float, SUM(CASE WHEN InjuryStatusCode IN ('101', '102', '103') THEN 1 ELSE 0 END)) AS 'SUSPECTED INJURIES',
              convert(float, SUM(CASE WHEN InjuryStatusCode = '104' THEN 1 ELSE 0 END)) AS 'NO APPARENT INJURIES'
            FROM FactPerson P INNER JOIN FactCrash C ON P.CrashSK = C.CrashPK
            WHERE C.CrashYear BETWEEN {start_year} AND {end_year}
            GROUP BY C.CrashYear, C.ParishCode
        ), 
        
        FACTSTATS AS (
            SELECT ReportYear, 
              FatalityCost,
              InjuryCost,
              NoInjuryCost,
              PDOCost
            FROM FactLAStats
        ),
        
        COSTS AS (
            SELECT INJURIES.YEAR AS 'YEAR',
              INJURIES.PARISH,
              CONVERT(FLOAT, SUM(INJURIES.FATALATIES) * SUM(FACTSTATS.FatalityCost) / 1000000.) AS 'FATALATY COST',
              CONVERT(FLOAT, SUM(INJURIES.[SUSPECTED INJURIES]) * SUM(FACTSTATS.InjuryCost) / 1000000.) AS 'INJURY COST',
              CONVERT(FLOAT, SUM(INJURIES.[NO APPARENT INJURIES]) * SUM(FACTSTATS.NoInjuryCost) / 1000000.) AS 'NO APPARENT INJURY COST',
              CONVERT(FLOAT, SUM(INJURIES.VEHICLES) * SUM(FACTSTATS.PDOCost) / 1000000.) AS 'VEHICLE COST'
            FROM INJURIES JOIN FACTSTATS ON INJURIES.YEAR = FACTSTATS.ReportYear
            GROUP BY INJURIES.YEAR, INJURIES.PARISH
        ), 
        
        LD AS (
            SELECT FactParishNormalizedData.YEAR, 
              PARISHCODE,
              CONVERT(FLOAT, SUM(LicensedDrivers)/1000.) AS 'LD'
            FROM FactParishNormalizedData 
            WHERE YEAR BETWEEN {start_year} AND {end_year}
            GROUP BY Year, ParishCode
        )
        
        SELECT COSTS.YEAR, 
          REFPARISH.ParishDescription AS 'Parish',
          ROUND(CONVERT(FLOAT, SUM([FATALATY COST] + [INJURY COST] + [NO APPARENT INJURY COST] + [VEHICLE COST])), 2) AS 'TOTAL COST IN MIL',
          ROUND(CONVERT(FLOAT, SUM(([FATALATY COST] + [INJURY COST] + [NO APPARENT INJURY COST] + [VEHICLE COST]) / [LD]) * 1000.), 0) AS 'COST PER LICENSED DRIVER'
        FROM COSTS INNER JOIN LD ON COSTS.PARISH = LD.ParishCode AND COSTS.YEAR = LD.Year
        INNER JOIN RefParish ON COSTS.PARISH = RefParish.ParishCode
        GROUP BY RefParish.ParishDescription, COSTS.YEAR
        ORDER BY RefParish.ParishDescription, COSTS.YEAR;
    """

    return pd.read_sql_query(sql_query, engine)


def create_alc_crash_df(engine, start_year, end_year):
    sql_query = f"""
        -- ALCOHOL-RELATED CRASHES
        WITH fatal_crash AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                ParishCode, 
                Parish,
                'Fatal' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            AND PredictedAlcohol = 1
            GROUP BY ParishCode, Parish, [CrashYear]
        ), 
        injury_crash AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                ParishCode, 
                Parish,
                'Injury' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            AND PredictedAlcohol = 1
            GROUP BY ParishCode, Parish, [CrashYear]
            
        ),
        fatal_crash_young_drivers AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                ParishCode, 
                Parish,
                'Fatal_young' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            AND PredictedAlcohol = 1
            AND YoungDriver = 1
            GROUP BY ParishCode, Parish, [CrashYear]
        
        ),
        injury_crash_young_drivers AS (	-- numbers are significantly off from previous report
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                ParishCode, 
                Parish,
                'Injury_young' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            AND PredictedAlcohol = 1
            AND YoungDriver = 1
            GROUP BY ParishCode, Parish, [CrashYear]
        
        )
        
        SELECT * FROM fatal_crash
        UNION ALL
        SELECT * FROM injury_crash
        UNION ALL
        SELECT * FROM fatal_crash_young_drivers
        UNION ALL
        SELECT * FROM injury_crash_young_drivers
        ORDER BY 'CrashType', Parish ASC, 'YEAR' ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_dwi_arrests_df(engine, start_year, end_year):
    sql_query = f"""
        WITH A AS (
            SELECT [Year] AS 'YEAR',
              Parish,
              SUM(CASE WHEN Age between 15 and 24 THEN 1 ELSE 0 END) AS 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24',
              COUNT(*) AS 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS'
            FROM FactCobra 
            WHERE DWICode IN ('2', '3')
            AND YEAR BETWEEN {start_year} AND {end_year}
            GROUP BY Parish, [Year]
        )
        
        SELECT A.YEAR,
          A.PARISH AS 'Parish',
          SUM(A.[NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24]) AS 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24',
          SUM(A.[NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS]) AS 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS',
          CONCAT(
            CAST(SUM(A.[NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24]) * 100. / SUM(A.[NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS]) AS DECIMAL(10, 2)), 
            '%' 
          ) AS 'PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'
        FROM A 
        GROUP BY PARISH, YEAR
        ORDER BY Parish, YEAR;
    """

    return pd.read_sql_query(sql_query, engine)


def create_ped_motor_bike_df(engine, start_year, end_year):
    sql_query = f"""
        WITH Fatalities AS (
            SELECT FC.CrashYear AS Year,
                FC.ParishCode,
                SUM(CASE WHEN FP.BodyTypeCode IN ('300', '301') THEN 1 ELSE 0 END) AS MotorcycleFatalities,
                SUM(CASE WHEN FP.PersonTypeCode = '200' THEN 1 ELSE 0 END) AS PedestrianFatalities,
                SUM(CASE WHEN FP.PersonTypeCode = '100' THEN 1 ELSE 0 END) AS BicycleFatalities,
                COUNT(DISTINCT FP.PersonPK) AS TotalFatalities
            FROM dbo.FactCrash FC
            INNER JOIN dbo.FactPerson FP ON FC.CrashPK = FP.CrashSK
            WHERE FC.CrashYear BETWEEN {start_year} AND {end_year}
            AND InjuryStatusCode = '100'
            GROUP BY FC.CrashYear, FC.ParishCode
        ),
        ParishDescriptions AS (
            SELECT 
                ParishCode,
                ParishDescription
            FROM 
                RefParish
        )
        SELECT F.Year AS 'YEAR',
          SUM(F.MotorcycleFatalities) AS MotorcycleFatalities,
          SUM(F.PedestrianFatalities) AS PedestrianFatalities,
          SUM(F.BicycleFatalities) AS BicycleFatalities,
          SUM(F.TotalFatalities) AS TotalFatalities,
          CONCAT(
            CAST(SUM(F.MotorcycleFatalities) * 100. / SUM(F.TotalFatalities) AS DECIMAL(10, 2)), 
            '%'
          ) AS MotorcycleFatalityPercentage,
          CONCAT(
            CAST(SUM(F.PedestrianFatalities) * 100. / SUM(F.TotalFatalities) AS DECIMAL(10, 2)), 
            '%'
          ) AS PedestrianFatalityPercentage,
          CONCAT(
            CAST(SUM(F.BicycleFatalities) * 100. / SUM(F.TotalFatalities) AS DECIMAL(10, 2)), 
            '%'
          ) AS BicycleFatalityPercentage,
          PD.ParishDescription AS Parish
        FROM Fatalities F
        LEFT JOIN ParishDescriptions PD ON F.ParishCode = PD.ParishCode
        GROUP BY F.Year, PD.ParishDescription
        ORDER BY PD.ParishDescription, F.Year;
    """

    return pd.read_sql_query(sql_query, engine)
