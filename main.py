import pyodbc
from sqlalchemy import create_engine
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet

import os
from PyPDF2 import PdfFileReader, PdfMerger

import create_df
import create_tables


def sql_connection():
    # Connecting to CARTS Database
    try:
        conn_str_db = (
            r"Driver={ODBC Driver 17 for SQL Server};"
            r"Server=prod-sqldw;"
            f"Database=ECRASHDW;"
            r"Trusted_Connection=yes;"
        )
        conn_db = pyodbc.connect(conn_str_db)
        cursor_db = conn_db.cursor()
    except:
        conn_str_db = (
            r"Driver={SQL Server};"
            r"Server=prod-sqldw;"
            f"Database=ECRASHDW;"
            r"Trusted_Connection=yes;"
        )
        conn_db = pyodbc.connect(conn_str_db)
        cursor_db = conn_db.cursor()

    return create_engine('mssql+pyodbc://', creator=lambda: conn_db)


# Function to create PDF
def create_pdf(filename, title, table1, table2, table3, table4, table5, table6, table7, table8):
    doc = SimpleDocTemplate(filename, pagesize=letter)
    elements = []

    # Title
    styles = getSampleStyleSheet()
    title_text = '<para align="center"><b>%s</b></para>' % title
    title_paragraph = Paragraph(title_text, styles["Title"])
    elements.append(title_paragraph)  # Add some space between title and table

    # Create table for acadia_1
    data = (table1 +
            [['FATAL INJURY AND PROPERTY DAMAGE ONLY (PDO) CRASHES']] +
            table2 +
            [['FATALITIES AND INJURIES']] +
            table3 +
            [['SAFETY BELT/HARNESS USE']] +
            table4 +
            [['COST ESTIMATES']] +
            table5 +
            [['ALCOHOL-RELATED CRASHES']] +
            table6 +
            table7 +
            [['PEDESTRIAN, MOTORCYCLE AND BICYCLE FATALITIES']] +
            table8)
    table = Table(data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#98d434")),  # Change color here
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Change text color to black
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, 3), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Set background of remaining rows to white
        ('GRID', (0, 0), (-1, -1), 1, colors.black),

        ('ALIGN', (0, 4), (-1, 4), 'CENTER'),
        ('SPAN', (0, 4), (-1, 4)),
        ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor("#98d434")),

        ('ALIGN', (0, 5), (0, 13), 'LEFT'),

        ('ALIGN', (0, 14), (-1, 14), 'CENTER'),
        ('SPAN', (0, 14), (-1, 14)),
        ('BACKGROUND', (0, 14), (-1, 14), colors.HexColor("#98d434")),

        ('ALIGN', (0, 15), (0, 19), 'LEFT'),

        ('ALIGN', (0, 20), (-1, 20), 'CENTER'),
        ('SPAN', (0, 20), (-1, 20)),
        ('BACKGROUND', (0, 20), (-1, 20), colors.HexColor("#98d434")),

        ('ALIGN', (0, 21), (0, 21), 'LEFT'),

        ('ALIGN', (0, 22), (-1, 22), 'CENTER'),
        ('SPAN', (0, 22), (-1, 22)),
        ('BACKGROUND', (0, 22), (-1, 22), colors.HexColor("#98d434")),

        ('ALIGN', (0, 23), (0, 24), 'LEFT'),

        ('ALIGN', (0, 25), (-1, 25), 'CENTER'),
        ('SPAN', (0, 25), (-1, 25)),
        ('BACKGROUND', (0, 25), (-1, 25), colors.HexColor("#98d434")),

        ('ALIGN', (0, 26), (0, 36), 'LEFT'),

        ('ALIGN', (0, 37), (-1, 37), 'CENTER'),
        ('SPAN', (0, 37), (-1, 37)),
        ('BACKGROUND', (0, 37), (-1, 37), colors.HexColor("#98d434")),

        ('ALIGN', (0, 38), (0, 43), 'LEFT'),
    ]))

    elements.append(table)
    doc.build(elements)


if __name__ == '__main__':
    # Establish initial connection to ECRASHDW in prod-sqldw
    engine_db = sql_connection()

    # Specify start and end years for query
    start_year = 2018
    end_year = 2022
    years = list(range(start_year, end_year + 1))

    licensed_driver_population_df = create_df.create_licensed_driver_df(engine_db, start_year, end_year)

    # Initialize an empty dictionary
    parish_dict = {}

    # Iterate over unique values in the 'ParishDescriptions' column
    for description in licensed_driver_population_df['ParishDescription'].unique():
        # Initialize an empty dictionary for each unique description
        parish_dict[description] = {}

    for parish in parish_dict.keys():
        df = licensed_driver_population_df[licensed_driver_population_df['ParishDescription'] == parish]
        parish_dict[parish]['licensed_driver'] = create_tables.create_licensed_driver_table(df, parish, years)

    total_crash_table_df = create_df.create_total_crash_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = total_crash_table_df[total_crash_table_df['Parish'] == parish]
        parish_dict[parish]['total_crash'] = create_tables.create_total_crash_table(df, years)

    fat_and_injury_df = create_df.create_fat_and_injury_df(engine_db, start_year, end_year)

    fat_and_injury_df = pd.merge(
        fat_and_injury_df,
        licensed_driver_population_df[['ParishCode', 'YEAR', 'LICENSED DRIVER POPULATION']],
        on=['ParishCode', 'YEAR'],
        how='left'
    )

    fat_and_injury_df['NUMBER OF X PER 100,000 LICENSED DRIVERS'] \
        = round(fat_and_injury_df['NUMBER OF X'] / fat_and_injury_df['LICENSED DRIVER POPULATION'] * 100000, 1)

    for parish in parish_dict.keys():
        df = fat_and_injury_df[fat_and_injury_df['Parish'] == parish]
        parish_dict[parish]['fat_and_injury'] = create_tables.create_fat_and_injury_table(df, years)

    safety_belt_df = create_df.create_safety_belt_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = safety_belt_df[safety_belt_df['Parish'] == parish]
        parish_dict[parish]['safety_belt'] = create_tables.create_safety_belt_table(df, years)

    # Code for COST ESTIMATES section
    cost_estimate_df = create_df.create_cost_estimate_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = cost_estimate_df[cost_estimate_df['Parish'] == parish]
        parish_dict[parish]['cost_estimate'] = create_tables.create_cost_estimate_table(df, years)

    # Code for first part of ALCOHOL-RELATED CRASHES section
    alc_crash_df = create_df.create_alc_crash_df(engine_db, start_year, end_year)

    alc_crash_totals_df = alc_crash_df[alc_crash_df['CrashType'].isin(['Fatal', 'Injury'])]
    alc_crash_young_df = alc_crash_df[alc_crash_df['CrashType'].isin(['Fatal_young', 'Injury_young'])]

    # Assuming df is your dataframe and 'CrashType' is the column you want to modify
    alc_crash_young_df['CrashType'] = alc_crash_young_df['CrashType'].str.replace('_young', '')

    alc_crash_totals_df = pd.merge(
        alc_crash_totals_df,
        total_crash_table_df[['ParishCode', 'YEAR', 'CrashType', 'NUMBER OF X CRASHES']],
        on=['ParishCode', 'YEAR', 'CrashType'],
        how='left'
    )

    alc_crash_young_df = pd.merge(
        alc_crash_young_df,
        total_crash_table_df[['ParishCode', 'YEAR', 'CrashType', 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24']],
        on=['ParishCode', 'YEAR', 'CrashType'],
        how='left'
    )

    # For alc_crash_totals_df
    alc_crash_totals_df['PERCENT OF ALCOHOL-RELATED X CRASHES'] = \
        (alc_crash_totals_df['NUMBER OF ALCOHOL-RELATED X CRASHES'] / alc_crash_totals_df['NUMBER OF X CRASHES']) * 100
    alc_crash_totals_df['PERCENT OF ALCOHOL-RELATED X CRASHES'] = \
        alc_crash_totals_df['PERCENT OF ALCOHOL-RELATED X CRASHES'].round(2)
    alc_crash_totals_df['PERCENT OF ALCOHOL-RELATED X CRASHES'] = \
        alc_crash_totals_df['PERCENT OF ALCOHOL-RELATED X CRASHES'].map('{:.2f}%'.format)

    # For alc_crash_young_df
    alc_crash_young_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'] = \
        (alc_crash_young_df['NUMBER OF ALCOHOL-RELATED X CRASHES'] / alc_crash_young_df[
            'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24']) * 100
    alc_crash_young_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'] = \
        alc_crash_young_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'].round(2)
    alc_crash_young_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'] = \
        alc_crash_young_df['PERCENT OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'].map('{:.2f}%'.format)

    alc_crash_young_df.rename(columns={'NUMBER OF ALCOHOL-RELATED X CRASHES': 'NUMBER OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'}, inplace=True)

    alc_crash_young_df.drop(columns=['Parish'], inplace=True)

    alc_crash_df = pd.merge(
        alc_crash_totals_df,
        alc_crash_young_df,
        on=['ParishCode', 'YEAR', 'CrashType'],
        how='left'
    )

    for parish in parish_dict.keys():
        df = alc_crash_df[alc_crash_df['Parish'] == parish]
        parish_dict[parish]['alc_crash'] = create_tables.create_alc_crash_table(df, years)

    dwi_arrests_df = create_df.create_dwi_arrests_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = dwi_arrests_df[dwi_arrests_df['Parish'] == parish]
        parish_dict[parish]['dwi_arrests'] = create_tables.create_dwi_arrests_table(df, years)

    ped_motor_bike_df = create_df.create_ped_motor_bike_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = ped_motor_bike_df[ped_motor_bike_df['Parish'] == parish]
        parish_dict[parish]['ped_motor_bike'] = create_tables.create_ped_motor_bike_table(df, years)

    # Create Parish_PDFs folder if it doesn't exist
    if not os.path.exists('Parish_PDFs'):
        os.makedirs('Parish_PDFs')

    # Generate PDFs for each parish
    for parish in parish_dict.keys():
        parish_1 = parish_dict[parish]['licensed_driver']
        parish_2 = parish_dict[parish]['total_crash']
        parish_3 = parish_dict[parish]['fat_and_injury']
        parish_4 = parish_dict[parish]['safety_belt']
        parish_5 = parish_dict[parish]['cost_estimate']
        parish_6 = parish_dict[parish]['alc_crash']
        parish_7 = parish_dict[parish]['dwi_arrests']
        parish_8 = parish_dict[parish]['ped_motor_bike']
        create_pdf(f'Parish_PDFs/{parish}.pdf', parish, parish_1, parish_2, parish_3, parish_4, parish_5, parish_6, parish_7, parish_8)

    # Concatenate PDFs
    pdf_merger = PdfMerger()
    pdf_files = sorted(os.listdir('Parish_PDFs'))
    for pdf_file in pdf_files:
        pdf_merger.append(f'Parish_PDFs/{pdf_file}')

    with open('Parish_measures.pdf', 'wb') as output_pdf:
        pdf_merger.write(output_pdf)

    pdf_merger.close()

    # Delete Parish_PDFs folder
    for pdf_file in pdf_files:
        os.remove(os.path.join('Parish_PDFs', pdf_file))
    os.rmdir('Parish_PDFs')

    """
    # Test for a single parish
    acadia_1 = parish_dict['ACADIA']['licensed_driver']
    acadia_2 = parish_dict['ACADIA']['total_crash']
    acadia_3 = parish_dict['ACADIA']['fat_and_injury']
    acadia_4 = parish_dict['ACADIA']['safety_belt']
    acadia_5 = parish_dict['ACADIA']['alc_crash']

    create_pdf('acadia.pdf', 'ACADIA', acadia_1, acadia_2, acadia_3, acadia_4, acadia_5)
    """

