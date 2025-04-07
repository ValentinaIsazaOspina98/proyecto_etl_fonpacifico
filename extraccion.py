import pandas as pd
def extraer_datos_csv(ruta_csv):
    df_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_csv
"""
def extraer_datos_matrix_proyectos(ruta_csv):
    df_matrizgestionproyectos_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_matrizgestionproyectos_csv
def extraer_datos_spgr_flujos(ruta_csv):
    df_spgr_cronograma_flujos_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_spgr_cronograma_flujos_csv
def extraer_datos_spgr_pagos(ruta_csv):
    df_spgr_pagos_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_spgr_pagos_csv
def extraer_datos_soul_recaudos(ruta_csv):
    df_recaudos_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_recaudos_csv
def extraer_datos_soul_facturacion(ruta_csv):
    df_facturacion_csv = pd.read_csv(ruta_csv, delimiter=";")
    return df_facturacion_csv
"""