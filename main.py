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
    Ingesta Matriz Proyectos
"""
ruta_csv = ruta_base + "MATRIZ_PROYECTOS//MATRIZ_PROYECTOS/MATRIZ GESTIÓN DE PROYECTOS 04-03-2025.csv"
df_matrizgestionproyectos_csv = extraccion.extraer_datos_csv(ruta_csv)
print(df_matrizgestionproyectos_csv)
columnas = df_matrizgestionproyectos_csv.columns.to_list()
print(columnas)
#----------------------------------------------------------------------------------------
"""
    Ingesta SOUL - Recaudos 2024
"""
ruta_csv = ruta_base + "SOUL//SOUL/RECAUDO2.csv"
df_recaudos_csv = extraccion.extraer_datos_csv(ruta_csv)
print(df_recaudos_csv)
columnas = df_recaudos_csv.columns.to_list()
print(columnas)
#----------------------------------------------------------------------------------------
"""
    Ingesta SOUL - Facturacion 2024
"""
ruta_csv = ruta_base + "SOUL//SOUL/EXCEL_FACTURACION_2024.csv"
df_facturacion_csv = extraccion.extraer_datos_csv(ruta_csv)
print(df_facturacion_csv)
columnas = df_facturacion_csv.columns.to_list()
print(columnas)
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
# TRANSFORMACION DE DATOS
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
df_matrizgestionproyectos_csv = transformacion.transformacion_filtrado_matriz_proyectos(df_matrizgestionproyectos_csv)
#df_matrizgestionproyectos_csv = transformacion.transformacion_filtrado_matrizgestionproyectos_2024(df_matrizgestionproyectos_csv)
df_matrizgestionproyectos_csv = transformacion.transformacion_filtro_estado_ejecucion_proyectos(df_matrizgestionproyectos_csv)
funciones.guardar_csv_datos('df_matrizgestionproyectos_csv.csv', df_matrizgestionproyectos_csv)
transformacion.crear_tabla_proyectos()
transformacion.almcenar_datos_proyectos(df_matrizgestionproyectos_csv)
#----------------------------------------------------------------------------------------
df_recaudos_csv = transformacion.transformacion_filtrado_recaudo_csv(df_recaudos_csv)
funciones.guardar_csv_datos('df_recaudos_csv.csv', df_recaudos_csv)
#----------------------------------------------------------------------------------------
df_facturacion_csv = transformacion.transformacion_filtrado_facturacion_csv(df_facturacion_csv)
funciones.guardar_csv_datos('df_facturacion_csv.csv', df_facturacion_csv)
#----------------------------------------------------------------------------------------
df_combinado_facturacion_recaudos = transformacion.transformacion_combinar_recaudo_facturacion(df_recaudos_csv, df_facturacion_csv)
funciones.guardar_csv_datos('df_combinado_facturacion_recaudos.csv', df_combinado_facturacion_recaudos)
df_combinado_facturacion_recaudos_matriz = transformacion.transformacion_combinar_recaudo_facturacion_proyectos(df_combinado_facturacion_recaudos, df_matrizgestionproyectos_csv)
transformacion.crear_tabla_facturacion_recaudos()
transformacion.almcenar_datos_facturacion_recaudos(df_combinado_facturacion_recaudos_matriz)
# seria el valor consignado menos el valor de las deducciones
transformacion.calcular_porc_5_sobre_total_regalias()
transformacion.calcularporc_5_sobre_valor_convenio_sin_aporte()
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
transformacion.crear_tabla_ingresos_egresos_x_dia()
transformacion.consolidar_total_ingresos_egresos_por_dia()
carga.crear_tabla_flujos_caja()
carga.calcular_flujos_caja()
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------