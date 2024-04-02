import pandas as pd


def create_licensed_driver_df(engine, start_year, end_year):
    sql_query = f"""
        SELECT CAST([Year] AS INT) AS 'YEAR',
          SUM(LicensedDrivers) AS 'LICENSED DRIVER POPULATION',
          SUM(YouthLicensedDrivers) AS 'LICENSED DRIVER POPULATION OF AGES 15-24',
          CONCAT(CAST(SUM(YouthLicensedDrivers) * 100. / SUM(LicensedDrivers) AS DECIMAL(10,2)), '%') 
            AS 'PERCENT OF 15-24 YEAR OLD DRIVERS'
        FROM FactParishNormalizedData FPND
        WHERE [Year] BETWEEN {start_year} AND {end_year}
        GROUP BY [Year]
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
                'Fatal' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            GROUP BY [CrashYear]
        ),
        InjuryCrashes AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(CrashSeverityCode) AS 'NUMBER OF X CRASHES', 
                SUM(CAST(YoungDriver AS smallint)) AS 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                CONCAT(CAST(SUM(CAST(YoungDriver AS smallint)) * 100./COUNT(CrashSeverityCode) AS DECIMAL(10, 2)), '%') 
                  AS 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                'Injury' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            GROUP BY [CrashYear]
        ),
        PDOCrashes AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(CrashSeverityCode) AS 'NUMBER OF X CRASHES', 
                SUM(CAST(YoungDriver AS smallint)) AS 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                CONCAT(CAST(SUM(CAST(YoungDriver AS smallint)) * 100./COUNT(CrashSeverityCode) AS DECIMAL(10, 2)), '%') 
                  AS 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                'PDO' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 104 -- (O) PROPERTY DAMAGE ONLY
            GROUP BY [CrashYear]
        )
        SELECT * FROM FatalCrashes
        UNION ALL
        SELECT * FROM InjuryCrashes
        UNION ALL
        SELECT * FROM PDOCrashes
        ORDER BY 'CrashType', [YEAR] ASC
    """

    return pd.read_sql_query(sql_query, engine)


def create_fat_and_injury_df(engine, start_year, end_year):
    sql_query = f"""
        WITH FatalitiesAndInjuries AS (
            SELECT 
                FP.CrashYear AS [YEAR],
                CASE 
                    WHEN InjuryStatusCode = 100 THEN 'Fatalities'
                    ELSE 'Injuries'
                END AS InjuryStatus,
                COUNT(CASE WHEN InjuryStatusCode = 100 THEN 1 END) AS NumberOfFatalities,
                COUNT(CASE WHEN InjuryStatusCode IN (101, 102, 103) THEN 1 END) AS NumberOfInjuries
            FROM FactPerson FP
            INNER JOIN FactCrash FC ON FP.CrashSK = FC.CrashPK
            WHERE FP.CrashYear BETWEEN {start_year} AND {end_year}
            GROUP BY FP.CrashYear, 
                CASE 
                    WHEN InjuryStatusCode = 100 THEN 'Fatalities'
                    ELSE 'Injuries'
                END
        )
        SELECT [YEAR], InjuryStatus,
            CASE 
                WHEN InjuryStatus = 'Fatalities' THEN NumberOfFatalities
                ELSE NumberOfInjuries
            END AS [NUMBER OF X]
        FROM FatalitiesAndInjuries
        ORDER BY InjuryStatus, [YEAR] ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_safety_belt_df(engine, start_year, end_year):
    sql_query = f"""
                WITH CrashesAndPersons AS (
            SELECT 
                fc.CrashYear AS [YEAR], 
                COUNT(DISTINCT(CASE WHEN fp.NonRestraintPerson = 1 THEN fp.PERSONPK END)) AS UNRESTRAINED_DRIVERS_KILLED, 
                COUNT(DISTINCT(fp.PERSONPK)) AS DRIVERS_KILLED
            FROM [dbo].[FactCrash] fc
            INNER JOIN [dbo].[FactPerson] fp ON fc.[CrashPK] = fp.[CrashSK]
            WHERE 
                fc.[CrashYear] BETWEEN {start_year} AND {end_year}
                AND fp.[PersonTypeCode] = '001'
                AND fp.[InjuryStatusCode] = '100'
            GROUP BY fc.CrashYear
        )
        SELECT 
            CAP.[YEAR] AS [YEAR],
            CONCAT(
                CAST(SUM(CAP.UNRESTRAINED_DRIVERS_KILLED) * 100. / SUM(CAP.DRIVERS_KILLED) AS DECIMAL(10, 2)),
                '%'
            ) AS 'PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT'
        FROM 
            CrashesAndPersons CAP
        GROUP BY 
            CAP.[YEAR]
        ORDER BY 
            CAP.[YEAR] ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_cost_estimate_df(engine, start_year, end_year):
    sql_query = f"""
        WITH INJURIES AS (
            SELECT COUNT(DISTINCT(VEHICLESK)) AS 'VEHICLES',
                C.CrashYear AS 'YEAR', 
                SUM(CASE WHEN InjuryStatusCode = '100' THEN 1 ELSE 0 END) AS 'FATALITIES',
                SUM(CASE WHEN InjuryStatusCode IN ('101', '102', '103') THEN 1 ELSE 0 END) AS 'SUSPECTED INJURIES',
                SUM(CASE WHEN InjuryStatusCode = '104' THEN 1 ELSE 0 END) AS 'NO APPARENT INJURIES'
            FROM FactPerson P INNER JOIN FactCrash C ON P.CrashSK = C.CrashPK
            WHERE C.CrashYear BETWEEN {start_year} AND {end_year}
            GROUP BY C.CrashYear
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
                (SUM(INJURIES.FATALITIES) / 1000000.) * SUM(FACTSTATS.FatalityCost) AS 'FATALITY COST',
                (SUM(INJURIES.[SUSPECTED INJURIES]) / 1000000.) * SUM(FACTSTATS.InjuryCost) AS 'INJURY COST',
                (SUM(INJURIES.[NO APPARENT INJURIES]) / 1000000.) * SUM(FACTSTATS.NoInjuryCost) AS 'NO APPARENT INJURY COST',
                (SUM(INJURIES.VEHICLES) / 1000000.) * SUM(FACTSTATS.PDOCost) AS 'VEHICLE COST'
            FROM INJURIES JOIN FACTSTATS ON INJURIES.YEAR = FACTSTATS.ReportYear
            GROUP BY INJURIES.YEAR
        ),
                 
        LD AS (
            SELECT FactParishNormalizedData.YEAR, 
                SUM(LicensedDrivers) * 1. / 1000 AS 'LD'
            FROM FactParishNormalizedData 
            WHERE YEAR BETWEEN {start_year} AND {end_year}
            GROUP BY Year
        )
                 
        SELECT COSTS.YEAR,
        CAST(SUM([FATALITY COST] + [INJURY COST] + [NO APPARENT INJURY COST] + [VEHICLE COST]) AS DECIMAL(10, 2)) AS 'TOTAL COST IN MIL',
        CAST(SUM(([FATALITY COST] + [INJURY COST] + [NO APPARENT INJURY COST] + [VEHICLE COST]) / [LD]) * 1000. AS DECIMAL(10, 0)) AS 'COST PER LICENSED DRIVER'
        FROM COSTS INNER JOIN LD ON COSTS.YEAR = LD.Year
        GROUP BY COSTS.YEAR
        ORDER BY COSTS.YEAR;
    """

    return pd.read_sql_query(sql_query, engine)


def create_alc_crash_df(engine, start_year, end_year):
    sql_query = f"""
        WITH fatal_crash AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                'Fatal' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            AND PredictedAlcohol = 1
            GROUP BY [CrashYear]
        ), 
        injury_crash AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                'Injury' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            AND PredictedAlcohol = 1
            GROUP BY [CrashYear]

        ),
        fatal_crash_young_drivers AS (
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                'Fatal_young' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode = 100 -- (K) Fatal Injury
            AND PredictedAlcohol = 1
            AND YoungDriver = 1
            GROUP BY [CrashYear]

        ),
        injury_crash_young_drivers AS (	
            SELECT [CrashYear] AS 'YEAR', 
                COUNT(*) AS 'NUMBER OF ALCOHOL-RELATED X CRASHES', 
                'Injury_young' AS 'CrashType'
            FROM FactCrash
            WHERE [CrashYear] BETWEEN {start_year} AND {end_year}
            AND CrashSeverityCode IN (101, 102, 103) -- (A) SUSPECTED SERIOUS INJURY (B) SUSPECTED MINOR INJURY (C) POSSIBLE INJURY
            AND PredictedAlcohol = 1
            AND YoungDriver = 1
            GROUP BY [CrashYear]

        )

        SELECT * FROM fatal_crash
        UNION ALL
        SELECT * FROM injury_crash
        UNION ALL
        SELECT * FROM fatal_crash_young_drivers
        UNION ALL
        SELECT * FROM injury_crash_young_drivers
        ORDER BY 'CrashType', 'YEAR' ASC;
    """

    return pd.read_sql_query(sql_query, engine)


def create_dwi_arrests_df(engine, start_year, end_year):
    sql_query = f"""
        WITH A AS (
            SELECT [Year] AS 'YEAR',
              SUM(CASE WHEN Age between 15 and 24 THEN 1 ELSE 0 END) AS 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24',
              COUNT(*) AS 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS'
            FROM FactCobra 
            WHERE DWICode IN ('2', '3')
            AND YEAR BETWEEN {start_year} AND {end_year}
            GROUP BY [Year]
        )

        SELECT A.YEAR,
          SUM(A.[NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24]) AS 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24',
          SUM(A.[NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS]) AS 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS',
          CONCAT(
            CAST(SUM(A.[NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24]) * 100. / SUM(A.[NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS]) AS DECIMAL(10, 2)), 
            '%' 
          ) AS 'PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'
        FROM A 
        GROUP BY YEAR
        ORDER BY YEAR;
    """

    return pd.read_sql_query(sql_query, engine)


def create_ped_motor_bike_df(engine, start_year, end_year):
    sql_query = f"""
        WITH Fatalities AS (
            SELECT FC.CrashYear AS Year,
                SUM(CASE WHEN FP.BodyTypeCode IN ('300', '301') THEN 1 ELSE 0 END) AS MotorcycleFatalities,
                SUM(CASE WHEN FP.PersonTypeCode = '200' THEN 1 ELSE 0 END) AS PedestrianFatalities,
                SUM(CASE WHEN FP.PersonTypeCode = '100' THEN 1 ELSE 0 END) AS BicycleFatalities,
                COUNT(DISTINCT FP.PersonPK) AS TotalFatalities
            FROM dbo.FactCrash FC
            INNER JOIN dbo.FactPerson FP ON FC.CrashPK = FP.CrashSK
            WHERE FC.CrashYear BETWEEN {start_year} AND {end_year}
            AND InjuryStatusCode = '100'
            GROUP BY FC.CrashYear
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
          ) AS BicycleFatalityPercentage
        FROM Fatalities F
        GROUP BY F.Year
        ORDER BY F.Year;
    """

    return pd.read_sql_query(sql_query, engine)


def create_trains_df(engine, start_year, end_year):
    sql_query = f"""
        SELECT C.CrashYear AS 'YEAR', 
            COUNT(DISTINCT CASE WHEN P.BodyTypeCode = '600' THEN C.CRASHPK END) AS 'TOTAL TRAIN CRASHES',
            CONCAT(
                CAST(COUNT(DISTINCT CASE WHEN P.BodyTypeCode = '600' THEN C.CRASHPK END) * 100. / COUNT(DISTINCT C.CRASHPK) AS DECIMAL(10, 2)),
                '%'
            ) AS 'PERCENT TRAIN CRASHES',
            SUM(CASE WHEN P.InjuryStatusCode = '100' AND C.RailRoadTrainInvolved = 1 THEN 1 ELSE 0 END) AS 'NUMBER OF TRAIN FATALITIES',
            CONCAT(
                CAST(ISNULL(SUM(CASE WHEN InjuryStatusCode = '100' AND C.RailRoadTrainInvolved = 1 THEN 1 ELSE 0 END) * 100. / NULLIF(COUNT(CASE WHEN InjuryStatusCode = '100' THEN 1 END), 0), 0) AS DECIMAL(10, 2)),
                '%'
            ) AS 'PERCENT OF TRAIN FATALITIES',
            SUM(CASE WHEN P.InjuryStatusCode IN ('101', '102', '103') AND C.RailRoadTrainInvolved = 1 THEN 1 ELSE 0 END) AS 'NUMBER OF TRAIN INJURIES',
            CONCAT(
                CAST(SUM(CASE WHEN InjuryStatusCode IN ('101', '102', '103') AND C.RailRoadTrainInvolved = 1 THEN 1 ELSE 0 END) * 100. / NULLIF(COUNT(CASE WHEN InjuryStatusCode IN ('101', '102', '103') THEN 1 END), 0) AS DECIMAL(10, 2)),
                '%'
            ) AS 'PERCENT OF TRAIN INJURIES'
        FROM
            (SELECT CrashYear, CrashPK, RailRoadTrainInvolved
             FROM FactCrash
             WHERE CrashYear BETWEEN {start_year} AND {end_year}) AS C
        INNER JOIN FactPerson AS P ON P.CrashSK = C.CrashPK
        GROUP BY C.CrashYear
        ORDER BY C.CrashYear
    """

    return pd.read_sql_query(sql_query, engine)


def create_com_mot_veh_df(engine, start_year, end_year):
    sql_query = f"""
        -- COMMERCIAL MOTOR VEHICLES (CMV)
        WITH cmv_fatal_crash AS (
        SELECT c.[CrashYear] AS 'YEAR',
            SUM(c.CrashCount) AS 'NUMBER OF CMV X CRASHES',
            CONCAT(CAST(SUM(c.CrashCount)* 100. /MIN(s.tot_count) AS DECIMAL(10,2)), '%')  as 'PERCENT OF CMV X CRASHES',
            'Fatal' AS 'CrashType'
        FROM FactCrash c 
            left join 
                (
                    select [CrashYear], count(1) as 'tot_count'
                    from FactCrash 
                    where [CrashYear] BETWEEN {start_year} AND {end_year}
                    AND CrashSeverityCode = '100' 
                    GROUP BY [CrashYear]
                ) as s 
            on c.CrashYear = s.CrashYear 
        WHERE c.[CrashYear] BETWEEN {start_year} AND {end_year}
        AND CrashSeverityCMVCode = '100'
        GROUP BY c.[CrashYear]

        ),
        cmv_injury_crash AS (
        SELECT c.[CrashYear] AS 'YEAR',
            COUNT (*) AS 'NUMBER OF CMV X CRASHES',
            CONCAT(CAST(SUM(c.CrashCount)* 100. /MIN(s.tot_count) AS DECIMAL(10,2)), '%')  as 'PERCENT OF CMV X CRASHES',
            'Injury' AS 'CrashType'
        FROM FactCrash c 
            join 
                (
                    select [CrashYear], count(1) as 'tot_count'
                    from FactCrash 
                    where [CrashYear] BETWEEN {start_year} AND {end_year}
                    AND CrashSeverityCode IN ('101', '102', '103')
                    GROUP BY [CrashYear]
                ) as s 
            on c.CrashYear = s.CrashYear
        WHERE c.[CrashYear] BETWEEN {start_year} AND {end_year}
        AND CrashSeverityCMVCode IN ('101', '102', '103')
        GROUP BY c.[CrashYear]

        ),
        cmv_pdo_crash AS (
        SELECT c.[CrashYear] AS 'YEAR',
            COUNT (*) AS 'NUMBER OF CMV X CRASHES',
            CONCAT(CAST(SUM(c.CrashCount)* 100. /MIN(s.tot_count) AS DECIMAL(10,2)), '%')  as 'PERCENT OF CMV X CRASHES',
            'PDO' AS 'CrashType'
        FROM FactCrash c 
            join 
                (
                    select [CrashYear], count(1) as 'tot_count'
                    from FactCrash 
                    where [CrashYear] BETWEEN {start_year} AND {end_year}
                    AND CrashSeverityCode = '104' 
                    GROUP BY [CrashYear]
                ) as s 
            on c.CrashYear = s.CrashYear
        WHERE c.[CrashYear] BETWEEN {start_year} AND {end_year}
        AND CrashSeverityCMVCode = '104'
        GROUP BY c.[CrashYear]
        )

        SELECT * FROM cmv_fatal_crash
        UNION ALL
        SELECT * FROM cmv_injury_crash
        UNION ALL
        SELECT * FROM cmv_pdo_crash
        ORDER BY 'CrashType', 'YEAR' ASC;
    """

    return pd.read_sql_query(sql_query, engine)
