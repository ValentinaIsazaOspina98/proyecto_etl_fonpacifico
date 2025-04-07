import pandas as pd
import openpyxl
import base_datos
from openpyxl.styles import Alignment, numbers
import sqlite3
from datetime import datetime
def crear_tabla_flujos_caja():
    base_datos.ejecutar_sql(base_datos.sql_crear_tabla_flujos_caja())
def calcular_flujos_caja():
    base_datos.ejecutar_sql(f"delete from apl1_flujo_caja")
    registros_database = base_datos.ejecutar_select("select FECHA, (COALESCE(PORC_5_SOBRE_TOTAL_REGALIAS,0) + COALESCE(PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE,0) + COALESCE(PORC_5_DEL_58_INTERADMINISTRATIVO,0)) as ingresos, (COALESCE(VALOR_APORTADO_CONVENIO_POR_FONPACI,0) + COALESCE(POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS,0)) as gatos from apl1_ingresos_egresos_x_dia")
    for datos in registros_database:
        fecha_str = datos[0]
        fecha_obj = datetime.strptime(fecha_str, '%d-%b-%y')  # Convierte a objeto datetime
        fecha_sqlite = fecha_obj.strftime('%m-%d-%Y')
        base_datos.ejecutar_sql(f"insert into apl1_flujo_caja(fecha, ingresos, egresos) values ('{fecha_sqlite}',{datos[1]},{datos[2]})")