import pyodbc
from sqlalchemy import create_engine
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

import os
from PyPDF2 import PdfMerger

import create_df
import create_louisiana_df
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
def create_pdf(filename, title, table1, table2, table3, table4, table5, table6, table7, table8, table9, table10):
    doc = SimpleDocTemplate(filename, pagesize=letter, leftMargin=0.5, rightMargin=0.5, topMargin=0.5, bottomMargin=0.5)
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
            table8 +
            [['TRAINS']] +
            table9 +
            [['COMMERCIAL MOTOR VEHICLES (CMV)']] +
            table10)

    table = Table(data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#98d434")),  # Change color here
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Change text color to black
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, 3), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Set background of remaining rows to white
        ('TOPPADDING', (0, 0), (-1, -1), 0.5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
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

        ('ALIGN', (0, 44), (-1, 44), 'CENTER'),
        ('SPAN', (0, 44), (-1, 44)),
        ('BACKGROUND', (0, 44), (-1, 44), colors.HexColor("#98d434")),

        ('ALIGN', (0, 45), (0, 50), 'LEFT'),

        ('ALIGN', (0, 51), (-1, 51), 'CENTER'),
        ('SPAN', (0, 51), (-1, 51)),
        ('BACKGROUND', (0, 51), (-1, 51), colors.HexColor("#98d434")),

        ('ALIGN', (0, 52), (0, 57), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
    ]))

    elements.append(table)

    # Append text at the end
    additional_text = ('Note: The percent change of NaN (not a number) represents when a percent change between '
                       'two years cannot be computed.')
    additional_style = ParagraphStyle(name='AdditionalText', parent=styles['Normal'], leftIndent=40, rightIndent=40)
    additional_paragraph = Paragraph(additional_text, additional_style)
    elements.append(additional_paragraph)

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
    state_dict = {'LOUISIANA': {}}

    # Iterate over unique values in the 'ParishDescriptions' column
    for description in licensed_driver_population_df['ParishDescription'].unique():
        # Initialize an empty dictionary for each unique description
        parish_dict[description] = {}

    la_licensed_driver_population_df = create_louisiana_df.create_licensed_driver_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['licensed_driver'] = create_tables.create_licensed_driver_table(la_licensed_driver_population_df, 'LOUISIANA', years)

    df = create_louisiana_df.create_total_crash_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['total_crash'] = create_tables.create_total_crash_table(df, years)

    fat_and_injury_df = create_louisiana_df.create_fat_and_injury_df(engine_db, start_year, end_year)

    fat_and_injury_df = pd.merge(
        fat_and_injury_df,
        la_licensed_driver_population_df[['YEAR', 'LICENSED DRIVER POPULATION']],
        on=['YEAR'],
        how='left'
    )

    fat_and_injury_df['NUMBER OF X PER 100,000 LICENSED DRIVERS'] \
        = round(fat_and_injury_df['NUMBER OF X'] / fat_and_injury_df['LICENSED DRIVER POPULATION'] * 100000, 1)

    state_dict['LOUISIANA']['fat_and_injury'] = create_tables.create_fat_and_injury_table(fat_and_injury_df, years)

    safety_belt_df = create_louisiana_df.create_safety_belt_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['safety_belt'] = create_tables.create_safety_belt_table(safety_belt_df, years)

    # Code for COST ESTIMATES section
    cost_estimate_df = create_louisiana_df.create_cost_estimate_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['cost_estimate'] = create_tables.create_cost_estimate_table(cost_estimate_df, years)

    alc_crash_df = create_louisiana_df.create_alc_crash_df(engine_db, start_year, end_year)

    alc_crash_totals_df = alc_crash_df[alc_crash_df['CrashType'].isin(['Fatal', 'Injury'])]
    alc_crash_young_df = alc_crash_df[alc_crash_df['CrashType'].isin(['Fatal_young', 'Injury_young'])]

    # Assuming df is your dataframe and 'CrashType' is the column you want to modify
    alc_crash_young_df['CrashType'] = alc_crash_young_df['CrashType'].str.replace('_young', '')

    alc_crash_totals_df = pd.merge(
        alc_crash_totals_df,
        df[['YEAR', 'CrashType', 'NUMBER OF X CRASHES']],
        on=['YEAR', 'CrashType'],
        how='left'
    )

    alc_crash_young_df = pd.merge(
        alc_crash_young_df,
        df[['YEAR', 'CrashType', 'NUMBER OF X CRASHES INVOLVING DRIVERS OF AGES 15-24']],
        on=['YEAR', 'CrashType'],
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

    alc_crash_young_df.rename(columns={
        'NUMBER OF ALCOHOL-RELATED X CRASHES': 'NUMBER OF ALCOHOL-RELATED X CRASHES INVOLVING DRIVERS AGES 15-24'},
                              inplace=True)

    alc_crash_df = pd.merge(
        alc_crash_totals_df,
        alc_crash_young_df,
        on=['YEAR', 'CrashType'],
        how='left'
    )

    state_dict['LOUISIANA']['alc_crash'] = create_tables.create_alc_crash_table(alc_crash_df, years)

    dwi_arrests_df = create_louisiana_df.create_dwi_arrests_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['dwi_arrests'] = create_tables.create_dwi_arrests_table(dwi_arrests_df, years)

    ped_motor_bike_df = create_louisiana_df.create_ped_motor_bike_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['ped_motor_bike'] = create_tables.create_ped_motor_bike_table(ped_motor_bike_df, years)

    trains_df = create_louisiana_df.create_trains_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['trains'] = create_tables.create_trains_table(trains_df, years)

    com_mot_veh_df = create_louisiana_df.create_com_mot_veh_df(engine_db, start_year, end_year)
    state_dict['LOUISIANA']['com_mot_veh'] = create_tables.create_com_mot_veh_table(com_mot_veh_df, years)

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

    trains_df = create_df.create_trains_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = trains_df[trains_df['Parish'] == parish]
        parish_dict[parish]['trains'] = create_tables.create_trains_table(df, years)

    com_mot_veh_df = create_df.create_com_mot_veh_df(engine_db, start_year, end_year)

    for parish in parish_dict.keys():
        df = com_mot_veh_df[com_mot_veh_df['Parish'] == parish]
        parish_dict[parish]['com_mot_veh'] = create_tables.create_com_mot_veh_table(df, years)

    # Create Parish_PDFs folder if it doesn't exist
    if not os.path.exists('Parish_PDFs'):
        os.makedirs('Parish_PDFs')

    """
    # Generate PDFs for each parish
    la_1 = state_dict['LOUISIANA']['licensed_driver']
    la_2 = state_dict['LOUISIANA']['total_crash']
    la_3 = state_dict['LOUISIANA']['fat_and_injury']
    la_4 = state_dict['LOUISIANA']['safety_belt']
    la_5 = state_dict['LOUISIANA']['cost_estimate']
    la_6 = state_dict['LOUISIANA']['alc_crash']
    la_7 = state_dict['LOUISIANA']['dwi_arrests']
    la_8 = state_dict['LOUISIANA']['ped_motor_bike']
    la_9 = state_dict['LOUISIANA']['trains']
    la_10 = state_dict['LOUISIANA']['com_mot_veh']
    create_pdf(f'Parish_PDFs/LOUISIANA.pdf', 'LOUISIANA', la_1, la_2, la_3, la_4, la_5, la_6, la_7, la_8, la_9, la_10)
    """

    state_dict.update(parish_dict)

    # Generate PDFs for each parish
    for parish in state_dict.keys():
        parish_1 = state_dict[parish]['licensed_driver']
        parish_2 = state_dict[parish]['total_crash']
        parish_3 = state_dict[parish]['fat_and_injury']
        parish_4 = state_dict[parish]['safety_belt']
        parish_5 = state_dict[parish]['cost_estimate']
        parish_6 = state_dict[parish]['alc_crash']
        parish_7 = state_dict[parish]['dwi_arrests']
        parish_8 = state_dict[parish]['ped_motor_bike']
        parish_9 = state_dict[parish]['trains']
        parish_10 = state_dict[parish]['com_mot_veh']
        create_pdf(f'Parish_PDFs/{parish}.pdf', parish, parish_1, parish_2, parish_3, parish_4, parish_5, parish_6, parish_7, parish_8, parish_9, parish_10)

    # Initialize PDF merger
    pdf_merger = PdfMerger()

    # List all PDF files in the directory
    pdf_files = sorted(os.listdir('Parish_PDFs'))

    # Separate 'LOUISIANA.PDF' from the rest of the PDFs
    louisiana_pdf = None
    other_pdfs = []

    for pdf_file in pdf_files:
        if pdf_file.upper() == 'LOUISIANA.PDF':
            louisiana_pdf = pdf_file
        else:
            other_pdfs.append(pdf_file)

    # If 'LOUISIANA.PDF' found, append it first
    if louisiana_pdf:
        pdf_merger.append(f'Parish_PDFs/{louisiana_pdf}')

    # Append the rest of the PDFs
    for pdf_file in other_pdfs:
        pdf_merger.append(f'Parish_PDFs/{pdf_file}')

    # Write the merged PDF
    with open('Parish_measures.pdf', 'wb') as output_pdf:
        pdf_merger.write(output_pdf)

    # Close the PDF merger
    pdf_merger.close()

    # Delete Parish_PDFs folder
    for pdf_file in pdf_files:
        os.remove(os.path.join('Parish_PDFs', pdf_file))
    os.rmdir('Parish_PDFs')

