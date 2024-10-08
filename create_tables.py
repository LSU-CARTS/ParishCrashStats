import numpy as np
import pandas as pd


def percent_change(df, col, penult_year, last_year):
    try:
        old_value = int(df[df['YEAR'] == penult_year][col].iloc[0])
        new_value = int(df[df['YEAR'] == last_year][col].iloc[0])
        return f'{round((new_value - old_value) / old_value * 100, 2)}%'
    except ValueError:
        try:
            old_value = float(df[df['YEAR'] == penult_year][col].iloc[0][:-1])
            new_value = float(df[df['YEAR'] == last_year][col].iloc[0][:-1])
            return f'{round(new_value - old_value, 2)}%'
        except TypeError:
            return np.nan
    except ZeroDivisionError:
        return 'NaN'


def create_licensed_driver_table(parish_df, parish, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(4)]

    table[0][0] = f'Parish: {parish}'
    table[0][1:len(years) + 1] = years
    table[0][-1] = f'% CHANGE {years[-2]} - {years[-1]}'

    table[1][0] = 'LICENSED DRIVER POPULATION'
    table[2][0] = 'LICENSED DRIVER POPULATION OF AGES 15-24'
    table[3][0] = 'PERCENT OF 15-24 YEAR OLD DRIVERS'

    for i in range(1, 4):
        table[i][1:-1] = parish_df[table[i][0]]
        table[i][-1] = percent_change(parish_df, table[i][0], years[-2], years[-1])

    for row in range(1, 3):
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_total_crash_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(9)]

    types_of_crashes = ['Fatal', 'Injury', 'PDO']
    index = 0

    for crash_type in types_of_crashes:
        crash_type_df = parish_df[parish_df['CrashType'] == crash_type]

        # Create a dataframe to hold the missing years
        missing_years_df = pd.DataFrame({'YEAR': years})

        # Merge with the original dataframe to identify missing years
        crash_type_df = pd.merge(missing_years_df, crash_type_df, on='YEAR', how='left')

        # Fill missing values with 0 for 'NUMBER OF X CRASHES' and 'NUMBER OF X CRASHES INVOLVING YOUTHS'
        crash_type_df['NUMBER OF X CRASHES'] = crash_type_df['NUMBER OF X CRASHES'].fillna(0)
        crash_type_df['NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24'] = crash_type_df[
            'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24'].fillna(0)

        # Sort the dataframe by 'YEAR'
        crash_type_df = crash_type_df.sort_values(by='YEAR')

        # Reset index
        crash_type_df.reset_index(drop=True, inplace=True)

        crash_type = crash_type.upper()

        table[index][0] = f'NUMBER OF {crash_type} CRASHES'
        table[index][1:-1] = crash_type_df['NUMBER OF X CRASHES']
        table[index][-1] = percent_change(crash_type_df, 'NUMBER OF X CRASHES', years[-2], years[-1])

        if crash_type == 'PDO':
            break

        table[index + 1][0] = f'NUMBER OF {crash_type} CRASHES INVOLVING DRIVERS OF AGES 15-24'
        table[index + 1][1:-1] = crash_type_df['NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24']
        table[index + 1][-1] = percent_change(crash_type_df, 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                                              years[-2], years[-1])

        table[index + 2][0] = f'PERCENT OF {crash_type} CRASHES INVOLVING DRIVERS OF AGES 15-24'
        table[index + 2][1:-1] = crash_type_df['PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24']
        table[index + 2][-1] = percent_change(crash_type_df, 'PERCENT OF X CRASHES INVOLVING DRIVERS OF AGES 15-24',
                                              years[-2], years[-1])

        # Blank line in table
        table[index + 3][:] = ''

        index += 4

    for row in [0, 1, 4, 5, 8]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_fat_and_injury_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(5)]

    types_of_injuries = ['Fatalities', 'Injuries']
    index = 0

    for injury_type in types_of_injuries:
        crash_type_df = parish_df[parish_df['InjuryStatus'] == injury_type]

        # Create a dataframe to hold the missing years
        missing_years_df = pd.DataFrame({'YEAR': years})

        # Merge with the original dataframe to identify missing years
        crash_type_df = pd.merge(missing_years_df, crash_type_df, on='YEAR', how='left')

        # Fill missing values with 0 for 'NUMBER OF X CRASHES' and 'NUMBER OF X CRASHES INVOLVING YOUTHS'
        crash_type_df['NUMBER OF X'] = crash_type_df['NUMBER OF X'].fillna(0)
        crash_type_df['NUMBER OF X PER 100,000 LICENSED DRIVERS'] = crash_type_df[
            'NUMBER OF X PER 100,000 LICENSED DRIVERS'].fillna(0.0)

        # Sort the dataframe by 'YEAR'
        crash_type_df = crash_type_df.sort_values(by='YEAR')

        # Reset index
        crash_type_df.reset_index(drop=True, inplace=True)

        injury_type = injury_type.upper()

        table[index][0] = f'NUMBER OF {injury_type}'
        table[index][1:-1] = crash_type_df['NUMBER OF X']
        table[index][-1] = percent_change(crash_type_df, 'NUMBER OF X', years[-2], years[-1])

        table[index + 1][0] = f'NUMBER OF {injury_type} PER 100,000 LICENSED DRIVERS'
        table[index + 1][1:-1] = crash_type_df['NUMBER OF X PER 100,000 LICENSED DRIVERS']
        table[index + 1][-1] = percent_change(crash_type_df, 'NUMBER OF X PER 100,000 LICENSED DRIVERS',
                                              years[-2], years[-1])

        if injury_type == 'FATALITIES':
            # Blank line in table
            table[index + 2][:] = ''
            index += 3

    for row in [0, 1, 3, 4]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_safety_belt_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(1)]

    # Create a dataframe to hold the missing years
    missing_years_df = pd.DataFrame({'YEAR': years})

    # Merge with the original dataframe to identify missing years
    parish_df = pd.merge(missing_years_df, parish_df, on='YEAR', how='left')

    # Fill missing values with 0 for 'NUMBER OF X CRASHES' and 'NUMBER OF X CRASHES INVOLVING YOUTHS'
    parish_df['PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT'] = parish_df['PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT'].fillna('0.00%')

    # Sort the dataframe by 'YEAR'
    parish_df = parish_df.sort_values(by='YEAR')

    # Reset index
    parish_df.reset_index(drop=True, inplace=True)

    table[0][0] = 'PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT'
    table[0][1:-1] = parish_df['PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT']
    table[0][-1] = percent_change(parish_df, 'PERCENT OF DRIVERS KILLED NOT WEARING SAFETY BELT', years[-2], years[-1])

    return table


def create_cost_estimate_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(2)]

    # Create a dataframe to hold the missing years
    missing_years_df = pd.DataFrame({'YEAR': years})

    # Merge with the original dataframe to identify missing years
    parish_df = pd.merge(missing_years_df, parish_df, on='YEAR', how='left')

    parish_df['TOTAL COST IN MIL'] = parish_df['TOTAL COST IN MIL'].fillna(0.00)
    parish_df['COST PER LICENSED DRIVER'] = parish_df['COST PER LICENSED DRIVER'].fillna(0)

    table[0][0] = 'TOTAL ESTIMATED COSTS OF TRAFFIC CRASHES (IN $1,000,000)'
    table[0][1:-1] = parish_df['TOTAL COST IN MIL']
    table[0][-1] = percent_change(parish_df, 'TOTAL COST IN MIL', years[-2], years[-1])

    table[1][0] = 'ESTIMATED COSTS OF TRAFFIC CRASHES PER LICENSED DRIVER'
    table[1][1:-1] = parish_df['COST PER LICENSED DRIVER']
    table[1][-1] = percent_change(parish_df, 'COST PER LICENSED DRIVER', years[-2], years[-1])

    for row in [0, 1]:
        for column in range(1, len(years) + 1):
            table[row][column] = "${:,}".format(table[row][column])

    return table


def create_alc_crash_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(8)]

    types_of_crashes = ['Fatal', 'Injury']
    index = 0

    for crash_type in types_of_crashes:
        crash_type_df = parish_df[parish_df['CrashType'] == crash_type]

        # Create a dataframe to hold the missing years
        missing_years_df = pd.DataFrame({'YEAR': years})

        # Merge with the original dataframe to identify missing years
        crash_type_df = pd.merge(missing_years_df, crash_type_df, on='YEAR', how='left')

        # Replace NaN values in numerical columns with 0
        numerical_columns = [
            'NUMBER OF ALCOHOL-RELATED X CRASHES',
            'NUMBER OF X CRASHES',
            'NUMBER OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24',
            'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24'
        ]
        crash_type_df[numerical_columns] = crash_type_df[numerical_columns].fillna(0)

        columns_to_replace = [
            'PERCENT OF ALCOHOL-RELATED X CRASHES',
            'PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'
        ]

        # Replace NaN values in specified columns with '0.00%'
        crash_type_df[columns_to_replace] = crash_type_df[columns_to_replace].fillna('0.00%')

        # Sort the dataframe by 'YEAR'
        crash_type_df = crash_type_df.sort_values(by='YEAR')

        # Reset index
        crash_type_df.reset_index(drop=True, inplace=True)

        if crash_type == 'Fatal':
            crash_type = 'Fatality'

        crash_type = crash_type.upper()

        table[index][0] = F'NUMBER OF ALCOHOL-RELATED {crash_type} CRASHES'
        table[index][1:-1] = crash_type_df['NUMBER OF ALCOHOL-RELATED X CRASHES']
        table[index][-1] = percent_change(crash_type_df, 'NUMBER OF ALCOHOL-RELATED X CRASHES', years[-2], years[-1])

        table[index + 1][0] = f'PERCENT OF ALCOHOL-RELATED {crash_type} CRASHES'
        table[index + 1][1:-1] = crash_type_df['PERCENT OF ALCOHOL-RELATED X CRASHES']
        table[index + 1][-1] = percent_change(crash_type_df, 'PERCENT OF ALCOHOL-RELATED X CRASHES', years[-2], years[-1])

        table[index + 4][0] = f'NUMBER OF ALCOHOL-RELATED {crash_type} CRASHES INVOLVING DRIVERS AGES 15-24'
        table[index + 4][1:-1] = crash_type_df['NUMBER OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24']
        table[index + 4][-1] = percent_change(crash_type_df, 'NUMBER OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24', years[-2], years[-1])

        table[index + 5][0] = f'PERCENT OF ALCOHOL-RELATED {crash_type} CRASHES INVOLVING DRIVERS AGES 15-24'
        table[index + 5][1:-1] = crash_type_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24']
        table[index + 5][-1] = percent_change(crash_type_df,
                                              'PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24',
                                              years[-2], years[-1])

        if index == 0:
            index += 2
        else:
            break

    for row in [0, 2, 4, 6]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_dwi_arrests_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(3)]

    # Create a dataframe to hold the missing years
    missing_years_df = pd.DataFrame({'YEAR': years})

    # Merge with the original dataframe to identify missing years
    parish_df = pd.merge(missing_years_df, parish_df, on='YEAR', how='left')

    numerical_columns = [
        'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24',
        'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS',
    ]

    parish_df[numerical_columns] = parish_df[numerical_columns].fillna(0)

    parish_df['PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'] = parish_df['PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'].fillna('0.00%')

    table[0][0] = 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'
    table[0][1:-1] = parish_df['NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24']
    table[0][-1] = percent_change(parish_df, 'NUMBER OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24', years[-2], years[-1])

    table[1][0] = 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS'
    table[1][1:-1] = parish_df['NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS']
    table[1][-1] = percent_change(parish_df, 'NUMBER OF DWI ARRESTS INVOLVING ALL DRIVERS', years[-2], years[-1])

    table[2][0] = 'PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24'
    table[2][1:-1] = parish_df['PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24']
    table[2][-1] = percent_change(parish_df, 'PERCENT OF DWI ARRESTS INVOLVING DRIVERS AGES 15-24', years[-2], years[-1])

    for row in [0, 1]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_ped_motor_bike_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(6)]

    # Merge with the original dataframe to identify missing years
    missing_years_df = pd.DataFrame({'YEAR': years})
    parish_df = pd.merge(missing_years_df, parish_df, on='YEAR', how='left')

    # Replace NaN values in numerical columns with 0
    freq_cols = [
        'MotorcycleFatalities',
        'PedestrianFatalities',
        'BicycleFatalities'
    ]

    rel_freq_cols = [
        'MotorcycleFatalityPercentage',
        'PedestrianFatalityPercentage',
        'BicycleFatalityPercentage'
    ]

    parish_df[freq_cols] = parish_df[freq_cols].fillna(0)
    parish_df[rel_freq_cols] = parish_df[rel_freq_cols].fillna('0.00%')

    table[0][0] = 'NUMBER OF PEDESTRIAN FATALITIES'
    table[0][1:-1] = parish_df['PedestrianFatalities']
    table[0][-1] = percent_change(parish_df, 'PedestrianFatalities', years[-2], years[-1])

    table[1][0] = 'PERCENT OF PEDESTRIAN FATALITIES'
    table[1][1:-1] = parish_df['PedestrianFatalityPercentage']
    table[1][-1] = percent_change(parish_df, 'PedestrianFatalityPercentage', years[-2], years[-1])

    table[2][0] = 'NUMBER OF MOTORCYCLE FATALITIES'
    table[2][1:-1] = parish_df['MotorcycleFatalities']
    table[2][-1] = percent_change(parish_df, 'MotorcycleFatalities', years[-2], years[-1])

    table[3][0] = 'PERCENT OF MOTORCYCLE FATALITIES'
    table[3][1:-1] = parish_df['MotorcycleFatalityPercentage']
    table[3][-1] = percent_change(parish_df, 'MotorcycleFatalityPercentage', years[-2], years[-1])

    table[4][0] = 'NUMBER OF BICYCLE FATALITIES'
    table[4][1:-1] = parish_df['BicycleFatalities']
    table[4][-1] = percent_change(parish_df, 'BicycleFatalities', years[-2], years[-1])

    table[5][0] = 'PERCENT OF BICYCLE FATALITIES'
    table[5][1:-1] = parish_df['BicycleFatalityPercentage']
    table[5][-1] = percent_change(parish_df, 'BicycleFatalityPercentage', years[-2], years[-1])

    for row in [0, 2, 4]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_trains_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(6)]

    # Merge with the original dataframe to identify missing years
    missing_years_df = pd.DataFrame({'YEAR': years})
    parish_df = pd.merge(missing_years_df, parish_df, on='YEAR', how='left')

    # Replace NaN values in numerical columns with 0
    freq_cols = ['TOTAL TRAIN CRASHES', 'NUMBER OF TRAIN FATALITIES', 'NUMBER OF TRAIN INJURIES']

    rel_freq_cols = ['PERCENT TRAIN CRASHES', 'PERCENT OF TRAIN FATALITIES', 'PERCENT OF TRAIN INJURIES']

    parish_df[freq_cols] = parish_df[freq_cols].fillna(0)
    parish_df[rel_freq_cols] = parish_df[rel_freq_cols].fillna('0.00%')

    table[0][0] = 'NUMBER OF TRAIN CRASHES'
    table[0][1:-1] = parish_df['TOTAL TRAIN CRASHES']
    table[0][-1] = percent_change(parish_df, 'TOTAL TRAIN CRASHES', years[-2], years[-1])

    table[1][0] = 'PERCENT OF TRAIN CRASHES'
    table[1][1:-1] = parish_df['PERCENT TRAIN CRASHES']
    table[1][-1] = percent_change(parish_df, 'PERCENT TRAIN CRASHES', years[-2], years[-1])

    table[2][0] = 'NUMBER OF TRAIN FATALITIES'
    table[2][1:-1] = parish_df['NUMBER OF TRAIN FATALITIES']
    table[2][-1] = percent_change(parish_df, 'NUMBER OF TRAIN FATALITIES', years[-2], years[-1])

    table[3][0] = 'PERCENT OF TRAIN FATALITIES'
    table[3][1:-1] = parish_df['PERCENT OF TRAIN FATALITIES']
    table[3][-1] = percent_change(parish_df, 'PERCENT OF TRAIN FATALITIES', years[-2], years[-1])

    table[4][0] = 'NUMBER OF TRAIN INJURIES'
    table[4][1:-1] = parish_df['NUMBER OF TRAIN INJURIES']
    table[4][-1] = percent_change(parish_df, 'NUMBER OF TRAIN INJURIES', years[-2], years[-1])

    table[5][0] = 'PERCENT OF TRAIN INJURIES'
    table[5][1:-1] = parish_df['PERCENT OF TRAIN INJURIES']
    table[5][-1] = percent_change(parish_df, 'PERCENT OF TRAIN INJURIES', years[-2], years[-1])

    for row in [0, 2, 4]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table


def create_com_mot_veh_table(parish_df, years):
    table = [[0 for _ in range(len(years) + 2)] for _ in range(6)]

    types_of_crashes = ['Fatal', 'Injury', 'PDO']
    index = 0

    for crash_type in types_of_crashes:
        crash_type_df = parish_df[parish_df['CrashType'] == crash_type]

        # Merge with the original dataframe to identify missing years
        missing_years_df = pd.DataFrame({'YEAR': years})
        crash_type_df = pd.merge(missing_years_df, crash_type_df, on='YEAR', how='left')

        # Replace NaN values in numerical columns with 0
        crash_type_df['NUMBER OF CMV X CRASHES'] = crash_type_df['NUMBER OF CMV X CRASHES'].fillna(0)
        crash_type_df['PERCENT OF CMV X CRASHES'] = crash_type_df['PERCENT OF CMV X CRASHES'].fillna('0.00%')

        table[index][0] = f'NUMBER OF CMV {crash_type.upper()} CRASHES'
        table[index][1:-1] = crash_type_df['NUMBER OF CMV X CRASHES']
        table[index][-1] = percent_change(crash_type_df, 'NUMBER OF CMV X CRASHES', years[-2], years[-1])

        table[index + 1][0] = f'PERCENT OF CMV {crash_type.upper()} CRASHES'
        table[index + 1][1:-1] = crash_type_df['PERCENT OF CMV X CRASHES']
        table[index + 1][-1] = percent_change(crash_type_df, 'PERCENT OF CMV X CRASHES', years[-2], years[-1])

        if types_of_crashes == 'PDO':
            break
        else:
            index += 2

    for row in [0, 2, 4]:
        for column in range(1, len(years) + 1):
            table[row][column] = "{:,}".format(table[row][column])

    return table
