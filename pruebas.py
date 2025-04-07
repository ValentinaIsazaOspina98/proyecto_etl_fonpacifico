import extraccion
import transformacion
import carga
import funciones
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
ruta_base = "C://Users//USUARIO//Documents//DesarrolloSoftwarePersonal//ProyectoEtlFonpacifico//FUENTES DATOS//"
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
# INGESTA Y EXTRACCION DE DATOS
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
"""
    Ingesta SOUL - Facturacion 2024
"""
ruta_csv = ruta_base + "SOUL//SOUL/EXCEL_FACTURACION_2024.csv"
df_facturacion_csv = extraccion.extraer_datos_csv(ruta_csv)
print(df_facturacion_csv)
columnas = df_facturacion_csv.columns.to_list()
print(columnas)