"""
This program reads gro_daily_excel files from local_path, load it into a mysql table 
and adds the field fecha_de_lectura as view_date

"""

import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

#* Cambiar ruta a ruta donde se guardaron los archivos extraidos del Mail

local_path = 'C:/Users/luism/Dropbox/My Documents/SimpleRMP/Ola/read_gro/attachments/'
engine = create_engine('mssql+pyodbc://srmp:Na?aMZLxF295FCd~4a@10.249.240.237:1433/POWERBI?driver=ODBC+Driver+17+for+SQL+Server', echo=False)

def extraer_fecha(nombre_archivo):
    partes=nombre_archivo
    for parte in partes:
        try:
            fecha = datetime.strptime(parte[19:31],'%Y%m%d%H%M')
            return fecha
        except ValueError:
            pass
    return None

files=os.listdir(local_path)

df_list = []

for archivo in files:
    if archivo.endswith('.xlsx') and ('gro_daily_property-' in archivo):
            df = pd.read_excel(os.path.join(local_path, archivo),sheet_name='Property')
            df['date'] = archivo
            df.columns=['Property Name','Day of Week','Occupancy Date','Special Event This Year','Physical Capacity This Year','Remaining Capacity This Year','Occupancy On Books This Year','Occupancy On Books STLY','Occupancy On Books ST2Y','User Projected Occupancy This Year','Rooms Sold - Group This Year','Rooms Sold - Group STLY','Rooms Sold - Group ST2Y','Rooms Sold - Transient This Year','Rooms Sold - Transient STLY','Rooms Sold - Transient ST2Y','Arrivals This Year','Departures This Year','Rooms N/A - Out of Order This Year','Rooms N/A - Other This Year','Cancelled This Year','No Show This Year','Booked Room Revenue This Year','Booked Room Revenue STLY','Booked Room Revenue ST2Y','Forecasted Room Revenue This Year','User Projected Revenue This Year','Occupancy Forecast - Total This Year','Occupancy Forecast - Group This Year','Occupancy Forecast - Transient This Year','System Total Demand - Total This Year','System Total Demand - Group This Year','System Total Demand - Transient This Year','User Total Demand - Total This Year','User Constrained Total Demand - Group This Year','User Unconstrained Total Demand - Transient This Year','Overbooking This Year','RevPAR On Books This Year','RevPAR Forecast This Year','ADR On Books This Year','ADR Forecast This Year','User Projected ADR This Year','Last Room Value This Year','Wash % This Year','LV0','Ola Santiago Providencia','Holiday Inn Express Santiago Las Condes','Pullman Santiago El Bosque','DoubleTree by Hilton Santiago - Vitacura','Novotel Santiago Providencia','date']            
            df_list.append(df)

merged_df = pd.concat(df_list, ignore_index=True)
merged_df['fecha_de_lectura'] = merged_df['date'].apply(lambda x:datetime.strptime(x[19:31],'%Y%m%d%H%M')+timedelta(hours=3))
merged_df = merged_df.sort_values(by='fecha_de_lectura')
#merged_df.drop(columns=['fecha'], inplace=True)
#merged_df.drop(columns=['date'], inplace=True)
#merged_df.drop(columns=['col1'], inplace=True)
merged_df = merged_df.dropna(subset=['LV0'])

valores_a_eliminar = ["Property Name","Day of Week","Occupancy Date","Special Event This Year","Physical Capacity This Year","Remaining Capacity This Year","Occupancy On Books This Year","Occupancy On Books STLY","Occupancy On Books ST2Y","User Projected Occupancy This Year","Rooms Sold - Group This Year","Rooms Sold - Group STLY","Rooms Sold - Group ST2Y","Rooms Sold - Transient This Year","Rooms Sold - Transient STLY","Rooms Sold - Transient ST2Y","Arrivals This Year","Departures This Year","Rooms N/A - Out of Order This Year","Rooms N/A - Other This Year","Cancelled This Year","No Show This Year","Booked Room Revenue This Year","Booked Room Revenue STLY","Booked Room Revenue ST2Y","Forecasted Room Revenue This Year","User Projected Revenue This Year","Occupancy Forecast - Total This Year","Occupancy Forecast - Group This Year","Occupancy Forecast - Transient This Year","System Total Demand - Total This Year","System Total Demand - Group This Year","System Total Demand - Transient This Year","User Total Demand - Total This Year","User Constrained Total Demand - Group This Year","User Unconstrained Total Demand - Transient This Year","Overbooking This Year","RevPAR On Books This Year","RevPAR Forecast This Year","ADR On Books This Year","ADR Forecast This Year","User Projected ADR This Year","Last Room Value This Year","Wash % This Year","LV0","Competitor Rate ForOla Santiago Providencia, Tapestry Colle This Year","Competitor Rate ForHoliday Inn Express Santiago Las Condes, This Year","Competitor Rate ForPullman Santiago El Bosque This Year","Competitor Rate ForDoubleTree by Hilton Santiago - Vitacura This Year","Competitor Rate ForNovotel Santiago Providencia This Year"
]

merged_df = merged_df.iloc[1:].reset_index(drop=True)
merged_df = merged_df.drop(merged_df[merged_df.isin(valores_a_eliminar)].dropna(how='all').index)
merged_df.reset_index(drop=True, inplace=True)

engine2 = create_engine('mysql+mysqlconnector://admin:efficientis@valle.mysql.simplermp.com:3306/OLA2', echo=False)


merged_df.to_sql('gro_daily_property', con=engine2, if_exists='replace', index=False)
merged_df.to_sql('gro_daily_property', con=engine, if_exists='replace', index=False)
#merged_df.to_csv('bookingdotcom_daily.csv', index=False)

"""
formatted_df = merged_df[[ 'Day', 'Date', 'Ola_Santiago_Providencia_Tapestry_Collection_by_Hilton', 'Novotel_Santiago_Providencia', 'DoubleTree_by_Hilton_Santiago_Vitacura', 'Pullman_Santiago_Vitacura', 'Pullman_Santiago_El_Bosque', 'Icon_Hotel', 'Holiday_Inn_Express_Santiago_Las_Condes_an_IHG_Hotel', 'Hotel_Nodo_Primer_hotel_explorador_urbano', 'DoubleTree_by_Hilton_Santiago_Kennedy_Chile', 'Hotel_Diego_de_Almagro_Providencia', 'source','fecha_de_lectura']]
formatted_df = formatted_df.melt(id_vars=['Day', 'Date','source','fecha_de_lectura'], var_name='Hotel', value_name='Rate')
formatted_df = formatted_df.sort_values(by=['Date'])
formatted_df = formatted_df.drop_duplicates()
formatted_df.to_sql('bookingdotcom_daily_rates_melted', con=engine2, if_exists='replace', index=False)
# Guardar en CSV y en base de datos
formatted_df.to_csv('daily_melted.csv', index=False)
"""