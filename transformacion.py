import pandas as pd
import base_datos
def transformacion_filtrado_matriz_proyectos(df_matrizgestionproyectos_csv):
    filtro = ['Estado Principal','Estado Derivados',' Valor Aporte de la Entidad ($)','N Proyecto / Contrato / Convenio / ','Entidad Contratante','Departamento','Año',' Valor Aporte de la Entidad ($).1','Porcentaje Aporte de la Entidad (%)','Valor Aporte de Fonpacífico ($)','Porcentaje Aporte de Fonpacífico (%)','Entidad del Derivado']
    df_matrizgestionproyectos_csv = df_matrizgestionproyectos_csv[filtro]
    return df_matrizgestionproyectos_csv
def transformacion_filtrado_matrizgestionproyectos_2024(df_matrizgestionproyectos_csv):
    filtro = df_matrizgestionproyectos_csv['Año'] == 2024
    df_matrizgestionproyectos_csv = df_matrizgestionproyectos_csv[filtro]
    return df_matrizgestionproyectos_csv
def transformacion_filtrado_recaudo_csv(df_recaudos_csv):
    filtro = ['fecha','factura','nombre','consignacion','deducciones','nombre']
    #df_recaudos_csv = df_recaudos_csv[filtro]
    return df_recaudos_csv
def transformacion_filtrado_facturacion_csv(df_facturacion_csv):
    filtro = ['codigo','fecha','subtotal','total','saldo','nom_tercero','abonos']
    #df_facturacion_csv = df_facturacion_csv[filtro]
    return df_facturacion_csv
def transformacion_combinar_recaudo_facturacion(df_recaudos_csv, df_facturacion_csv):
    df_combinado_facturacion_recaudos = pd.merge(df_recaudos_csv, df_facturacion_csv, left_on='factura', right_on='codigo', how='inner')
    return df_combinado_facturacion_recaudos
def transformacion_combinar_recaudo_facturacion_proyectos(df_combinado_facturacion_recaudos, df_matrizgestionproyectos_csv):
    df_combinado_facturacion_recaudos_matriz = pd.merge(df_combinado_facturacion_recaudos, df_matrizgestionproyectos_csv, left_on='nom_tercero', right_on='Entidad del Derivado', how='inner')
    return df_combinado_facturacion_recaudos_matriz
def transformacion_limpieza_recaudo_facturacion_proyectos(df_combinado_facturacion_recaudos_matriz):
    #eliminar_columnas = ['nom_tercero', 'nom_tercero.1', 'Entidad del Derivado','Departamento','Año','nombre.1',' Valor Aporte de la Entidad ($).1','Estado Derivados','subtotal']
    eliminar_columnas = ['nom_tercero', 'Entidad del Derivado','Departamento','Año',' Valor Aporte de la Entidad ($).1','Estado Derivados','subtotal']
    df_combinado_facturacion_recaudos_matriz = df_combinado_facturacion_recaudos_matriz.drop(columns = eliminar_columnas)
    return df_combinado_facturacion_recaudos_matriz
def transformacion_filtro_estado_derivado_recaudo_facturacion_proyectos(df_combinado_facturacion_recaudos_matriz):
    df_combinado_facturacion_recaudos_matriz = df_combinado_facturacion_recaudos_matriz[df_combinado_facturacion_recaudos_matriz['Estado Principal'] == 'Ejecución']
    return df_combinado_facturacion_recaudos_matriz
def transformacion_filtro_estado_ejecucion_proyectos(df_matrizgestionproyectos_csv):
    df_matrizgestionproyectos_csv = df_matrizgestionproyectos_csv[df_matrizgestionproyectos_csv['Estado Principal'] == 'Ejecución']
    return df_matrizgestionproyectos_csv
def transformacion_reorganizar_columnas(df_combinado_facturacion_recaudos_matriz):
    nuevo_orden_columnas = ['N Proyecto / Contrato / Convenio / ', 'Estado Principal', 'Entidad Contratante', ' Valor Aporte de la Entidad ($)', 'Porcentaje Aporte de la Entidad (%)','Valor Aporte de Fonpacífico ($)','Porcentaje Aporte de Fonpacífico (%)','nombre','codigo','fecha_y',
                            'total','saldo','abonos','factura','fecha_x','consignacion','deducciones']
    df_combinado_facturacion_recaudos_matriz = df_combinado_facturacion_recaudos_matriz[nuevo_orden_columnas]
    return df_combinado_facturacion_recaudos_matriz
def limpiar_monto(valor):
    if isinstance(valor, str):
        return round(float(valor.replace("$", "").replace(".", "").replace(",", ".")), 2)
    elif isinstance(valor, (int, float)):  # Si ya es numérico, solo redondear
        return round(valor, 2)
    else:
        return None  # Manejo de valores inesperados
def transformacion_eliminar_formato_predeterminado_columnas_monetarias(df_combinado_facturacion_recaudos_matriz):
    columnas_monetarias = [" Valor Aporte de la Entidad ($)", "Valor Aporte de Fonpacífico ($)"]
    df_montos_limpios = df_combinado_facturacion_recaudos_matriz[columnas_monetarias].applymap(limpiar_monto)
    df_combinado_facturacion_recaudos_matriz[columnas_monetarias] = df_montos_limpios
    return df_combinado_facturacion_recaudos_matriz
def limpiar_numero(valor):
    return round(float(valor.replace(",", ".")), 2) if isinstance(valor, str) else round(valor, 2)
def transformacion_eliminar_formato_predeterminado_columnas_numericas(df_combinado_facturacion_recaudos_matriz):
    columnas_numericas = ["total", "abonos", "consignacion", "deducciones"]
    df_numeros_limpios = df_combinado_facturacion_recaudos_matriz[columnas_numericas].applymap(limpiar_numero)
    df_combinado_facturacion_recaudos_matriz[columnas_numericas] = df_numeros_limpios
    return df_combinado_facturacion_recaudos_matriz
def limpiar_saldo(valor):
    if isinstance(valor, str) and valor.replace(",", "").replace(".", "").isdigit():
        return round(float(valor.replace(",", ".")), 2)  # Convierte texto a float con punto decimal
    elif isinstance(valor, (int, float)):
        return round(valor, 2)  # Si ya es numérico, solo redondea
    else:
        return 0.00  # Si no es número válido, devolver 0.00
def transformacion_eliminar_formato_predeterminado_columnas_saldos(df_combinado_facturacion_recaudos_matriz):
    df_saldo_limpio = df_combinado_facturacion_recaudos_matriz["saldo"].apply(limpiar_saldo)
    df_combinado_facturacion_recaudos_matriz["saldo"]  = df_saldo_limpio
    return df_combinado_facturacion_recaudos_matriz
def transformacion_renombrar_columnas(df_combinado_facturacion_recaudos_matriz):
    nuevos_nombres_columnas = [
        'ID PROYECTO / ID CONTRATO / ID CONVENIO',
        'ESTADO',
        'ENTIDAD CONTRATANTE',
        'VALOR APORTE DE LA ENTIDAD',
        'PORCENTAJE APORTE DE LA ENTIDAD',
        'VALOR APORTE FONPACIFICO',
        'PORCENTAJE APORTE FONPACIFICO',
        'ENTIDAD DEL DERIVADO',
        'CODIGO FACTURACION',
        'FECHA FACTURACION',
        'VALOR FACTURACION',
        'VALOR EN SALDO',
        'VALOR DE ABONOS',
        'CODIGO RECAUDO',
        'FECHA DE RECAUDACION',
        'VALOR CONSIGNADO',
        'VALOR DEDUCCIONES'
    ]
    df_combinado_facturacion_recaudos_matriz.columns = nuevos_nombres_columnas
    return df_combinado_facturacion_recaudos_matriz
def transformacion_columnas_no_deseadas(df_combinado_facturacion_recaudos_matriz):
    columnas_excluir = ["TASA DE RECAUDO (%)", "ÍNDICE DE LIQUIDEZ"]
    df_combinado_facturacion_recaudos_matriz = df_combinado_facturacion_recaudos_matriz.drop(columns=columnas_excluir, errors="ignore")
    return df_combinado_facturacion_recaudos_matriz
def aplicar_formato_moneda(valor):
    if isinstance(valor, (int, float)):
        return f"${valor:,.2f}"  # Agrega $ y mantiene el formato numérico
    return valor
def transformacion_aplicar_formato_moneda(df_combinado_facturacion_recaudos_matriz):
    columnas_monetarias = [
        'VALOR APORTE DE LA ENTIDAD', 'VALOR APORTE FONPACIFICO',
        'VALOR FACTURACION', 'VALOR EN SALDO', 'VALOR DE ABONOS',
        'VALOR CONSIGNADO', 'VALOR DEDUCCIONES'
    ]
    for columna in columnas_monetarias:
        if columna in df_combinado_facturacion_recaudos_matriz.columns:
            df_combinado_facturacion_recaudos_matriz[columna] = df_combinado_facturacion_recaudos_matriz[columna].apply(aplicar_formato_moneda)
    return df_combinado_facturacion_recaudos_matriz
def formatear_valores(valor):
    valor = valor.split(",")[0]
    valor = valor.replace("$", "").replace(".", "").replace(" ", "")
    return valor
def transformacion_valores_dinero(df_combinado_facturacion_recaudos_matriz):
    df_combinado_facturacion_recaudos_matriz['VALOR CONSIGNADO'] = df_combinado_facturacion_recaudos_matriz['VALOR CONSIGNADO'].apply(formatear_valores)
    #df_combinado_facturacion_recaudos_matriz['VALOR CONSIGNADO'] = formatear_valores(df_combinado_facturacion_recaudos_matriz['VALOR CONSIGNADO'])
    return df_combinado_facturacion_recaudos_matriz
def crear_tabla_proyectos():
    base_datos.ejecutar_sql(base_datos.sql_crear_tabla_proyectos())
def almcenar_datos_proyectos(df_matrizgestionproyectos_csv):
    sql, diccionario_registros = base_datos.organizar_sql_insert_apl1_proyectos(df_matrizgestionproyectos_csv)
    base_datos.ejecutar_sql("delete from apl1_proyectos")
    base_datos.ejecutar_sql_dicionario(sql, diccionario_registros)
def crear_tabla_facturacion_recaudos():
    base_datos.ejecutar_sql(base_datos.sql_crear_tabla_facturacion_recaudos())
def almcenar_datos_facturacion_recaudos(df_combinado_facturacion_recaudos_matriz):
    sql, diccionario_registros = base_datos.organizar_sql_insert_apl1_facturacion_recaudos(df_combinado_facturacion_recaudos_matriz)
    base_datos.ejecutar_sql("delete from apl1_facturacion_recaudos")
    base_datos.ejecutar_sql_dicionario(sql, diccionario_registros)
def consultar_datos_apl1_facturacion_recaudos():
    registros_database = base_datos.ejecutar_select("select * from apl1_facturacion_recaudos")
    return registros_database
def calcular_porc_5_sobre_total_regalias():
    registros_database = base_datos.ejecutar_select(f"select CODIGO_FACTURACION, VALOR_CONSIGNADO, round(((VALOR_CONSIGNADO - VALOR_DEDUCCIONES) * 0.05),2) as PORC_5_SOBRE_TOTAL_REGALIAS from apl1_facturacion_recaudos where ID_PROYECTO_CONTRATO_CONVENIO like '%BPIN%'")
    for datos in registros_database:
        print(f"Datos DataBase PORC_5_SOBRE_TOTAL_REGALIAS ------> : {datos}")
        base_datos.ejecutar_sql(f"update apl1_facturacion_recaudos set PORC_5_SOBRE_TOTAL_REGALIAS = {datos[2]} where CODIGO_FACTURACION = '{datos[0]}';")
def calcularporc_5_sobre_valor_convenio_sin_aporte():
    registros_database = base_datos.ejecutar_select("select CODIGO_FACTURACION, VALOR_CONSIGNADO, round(((VALOR_CONSIGNADO - VALOR_DEDUCCIONES) * 0.05),2) as PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE from apl1_facturacion_recaudos where ID_PROYECTO_CONTRATO_CONVENIO like '%CONVENIO%'")
    for datos in registros_database:
        print(f"Datos DataBase PORC_5_SOBRE_VALOR_APORTE_ENTIDAD ------> : {datos}")
        base_datos.ejecutar_sql(f"update apl1_facturacion_recaudos set PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE = {datos[2]} where CODIGO_FACTURACION = '{datos[0]}';")
    registros_database = base_datos.ejecutar_select("select distinct(ID_PROYECTO_CONTRATO_CONVENIO) from apl1_facturacion_recaudos where ID_PROYECTO_CONTRATO_CONVENIO like '%CONVENIO%'")
    for datos in registros_database:
        sql = f"""select fr.CODIGO_FACTURACION, pr.VALOR_APORTE_FONPACIFICO from apl1_facturacion_recaudos  fr, apl1_proyectos pr where fr.ID_PROYECTO_CONTRATO_CONVENIO = pr.ID_PROYECTO_CONTRATO_CONVENIO and fr.ID_PROYECTO_CONTRATO_CONVENIO = '{datos[0]}' order by fr.FECHA_DE_RECAUDACION desc limit 1;"""
        registros_database = base_datos.ejecutar_select(sql)
        for datos in registros_database:
            VALOR_APORTADO_CONVENIO_POR_FONPACI = formatear_valores(datos[1])
            print(f"Datos DataBase VALOR_APORTADO_CONVENIO_POR_FONPACI ------> : {datos}")
            base_datos.ejecutar_sql(f"update apl1_facturacion_recaudos set VALOR_APORTADO_CONVENIO_POR_FONPACI = {VALOR_APORTADO_CONVENIO_POR_FONPACI} where CODIGO_FACTURACION = '{datos[0]}';")
def calcular_porc_5_del_58_interadministrativo():
    registros_database = base_datos.ejecutar_select("select CODIGO_FACTURACION, VALOR_CONSIGNADO, round(((VALOR_CONSIGNADO * 0.58) * 0.05),2) as PORC_5_DEL_58_INTERADMINISTRATIVO from apl1_facturacion_recaudos_matriz where ID_PROYECTO_CONTRATO_CONVENIO like '%CONVENIO%'")
    for datos in registros_database:
        print(f"Datos DataBase PORC_5_DEL_58_INTERADMINISTRATIVO ------> : {datos}")
        base_datos.ejecutar_sql(f"update apl1_facturacion_recaudos_matriz set PORC_5_DEL_58_INTERADMINISTRATIVO = {datos[2]} where CODIGO_FACTURACION = '{datos[0]}';")
def calcular_por_42_deducible_de_interadminstrativos():
    registros_database = base_datos.ejecutar_select("select CODIGO_FACTURACION, round((VALOR_CONSIGNADO * 0.42),2) as POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS from apl1_facturacion_recaudos_matriz where ID_PROYECTO_CONTRATO_CONVENIO like '%CONVENIO%'")
    for datos in registros_database:
        print(f"Datos DataBase POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS ------> : {datos}")
        base_datos.ejecutar_sql(f"update apl1_facturacion_recaudos_matriz set POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS = {datos[1]} where CODIGO_FACTURACION = '{datos[0]}';")
def crear_tabla_ingresos_egresos_x_dia():
    base_datos.ejecutar_sql(base_datos.sql_crear_tabla_ingresos_egresos_x_dia())
def consolidar_total_ingresos_egresos_por_dia():
    base_datos.ejecutar_sql("delete from apl1_ingresos_egresos_x_dia")
    registros_database = base_datos.ejecutar_select("select distinct(FECHA_DE_RECAUDACION) from apl1_facturacion_recaudos")
    for datos in registros_database:
        base_datos.ejecutar_sql(f"insert into apl1_ingresos_egresos_x_dia(FECHA) values('{datos[0]}')")
    registros_database = base_datos.ejecutar_select("select FECHA from apl1_ingresos_egresos_x_dia")
    for datos in registros_database:
        PORC_5_SOBRE_TOTAL_REGALIAS = base_datos.ejecutar_select(f"select COALESCE(SUM(PORC_5_SOBRE_TOTAL_REGALIAS),0) from apl1_facturacion_recaudos where FECHA_DE_RECAUDACION = '{datos[0]}'")
        for porcentaje in PORC_5_SOBRE_TOTAL_REGALIAS:
            base_datos.ejecutar_sql(f"update apl1_ingresos_egresos_x_dia set PORC_5_SOBRE_TOTAL_REGALIAS = {porcentaje[0]} where FECHA = '{datos[0]}';")
        PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE = base_datos.ejecutar_select(f"select COALESCE(SUM(PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE),0) from apl1_facturacion_recaudos where FECHA_DE_RECAUDACION = '{datos[0]}'")
        for porcentaje in PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE:
            base_datos.ejecutar_sql(f"update apl1_ingresos_egresos_x_dia set PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE = {porcentaje[0]} where FECHA = '{datos[0]}';")
        VALOR_APORTADO_CONVENIO_POR_FONPACI = base_datos.ejecutar_select(f"select COALESCE(SUM(VALOR_APORTADO_CONVENIO_POR_FONPACI),0) from apl1_facturacion_recaudos where FECHA_DE_RECAUDACION = '{datos[0]}'")
        for porcentaje in VALOR_APORTADO_CONVENIO_POR_FONPACI:
            base_datos.ejecutar_sql(f"update apl1_ingresos_egresos_x_dia set VALOR_APORTADO_CONVENIO_POR_FONPACI = {porcentaje[0]} where FECHA = '{datos[0]}';")
        """
        PORC_5_DEL_58_INTERADMINISTRATIVO = base_datos.ejecutar_select(f"select COALESCE(SUM(PORC_5_DEL_58_INTERADMINISTRATIVO),0) from apl1_facturacion_recaudos where FECHA_DE_RECAUDACION = '{datos[0]}'")
        for porcentaje in PORC_5_DEL_58_INTERADMINISTRATIVO:
            base_datos.ejecutar_sql(f"update apl1_ingresos_egresos_x_dia set PORC_5_DEL_58_INTERADMINISTRATIVO = {porcentaje[0]} where FECHA = '{datos[0]}';")
        POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS = base_datos.ejecutar_select(f"select COALESCE(SUM(POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS),0) from apl1_facturacion_recaudos where FECHA_DE_RECAUDACION = '{datos[0]}'")
        for porcentaje in POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS:
            base_datos.ejecutar_sql(f"update apl1_ingresos_egresos_x_dia set POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS = {porcentaje[0]} where FECHA = '{datos[0]}';")
            ...
        """