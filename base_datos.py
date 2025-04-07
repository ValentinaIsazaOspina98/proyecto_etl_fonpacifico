import sqlite3
from datetime import datetime
def ejecutar_sql(sql):
    conexion = sqlite3.connect('database_flujos_caja.db')
    cursor = conexion.cursor()
    print(f"sql ------> : {sql}")
    cursor.execute(sql)
    conexion.commit()
    conexion.close()
def ejecutar_sql_dicionario(sql, lista_diccionarios):
    conexion = sqlite3.connect('database_flujos_caja.db')
    for registro in lista_diccionarios:
        cursor = conexion.cursor()
        print(f"sql ------> : {sql}")
        print(registro)
        cursor.execute(sql, registro)
    conexion.commit()
    conexion.close()
def ejecutar_select(sql):
    conexion = sqlite3.connect('database_flujos_caja.db')
    cursor = conexion.cursor()
    print(f"sql ------> : {sql}")
    cursor.execute(sql)
    resultado = cursor.fetchall()
    conexion.close()
    return resultado
def sql_crear_tabla_proyectos():
    sql = """CREATE TABLE IF NOT EXISTS apl1_proyectos(ID INTEGER PRIMARY KEY
            ,   ID_PROYECTO_CONTRATO_CONVENIO TEXT
            ,   ESTADO TEXT
            ,   ENTIDAD_CONTRATANTE TEXT
            ,   VALOR_APORTE_DE_LA_ENTIDAD INTEGER
            ,   PORCENTAJE_APORTE_DE_LA_ENTIDAD INTEGER
            ,   VALOR_APORTE_FONPACIFICO INTEGER
            ,   PORCENTAJE_APORTE_FONPACIFICO INTEGER
            ,   ENTIDAD_DEL_DERIVADO TEXT
            )
        """
    return sql
def organizar_sql_insert_apl1_proyectos(df_matrizgestionproyectos_csv):
    lista_registros = []
    diccionario_registros = {}
    consecutivo = 0
    sql = f"""INSERT INTO apl1_proyectos(ID
        ,   ID_PROYECTO_CONTRATO_CONVENIO
        ,   ESTADO
        ,   ENTIDAD_CONTRATANTE
        ,   VALOR_APORTE_DE_LA_ENTIDAD
        ,   PORCENTAJE_APORTE_DE_LA_ENTIDAD
        ,   VALOR_APORTE_FONPACIFICO
        ,   PORCENTAJE_APORTE_FONPACIFICO
        ,   ENTIDAD_DEL_DERIVADO
        ) VALUES(:ID
        ,   :ID_PROYECTO_CONTRATO_CONVENIO
        ,   :ESTADO
        ,   :ENTIDAD_CONTRATANTE
        ,   :VALOR_APORTE_DE_LA_ENTIDAD
        ,   :PORCENTAJE_APORTE_DE_LA_ENTIDAD
        ,   :VALOR_APORTE_FONPACIFICO
        ,   :PORCENTAJE_APORTE_FONPACIFICO
        ,   :ENTIDAD_DEL_DERIVADO
        );
    """
    for registro in df_matrizgestionproyectos_csv.to_dict(orient='records'):
        diccionario_registros = {}
        diccionario_registros["ID"] = consecutivo
        diccionario_registros["ID_PROYECTO_CONTRATO_CONVENIO"] = registro["N Proyecto / Contrato / Convenio / "]
        diccionario_registros["ESTADO"] = registro["Estado Principal"]
        diccionario_registros["ENTIDAD_CONTRATANTE"] = registro["Entidad Contratante"]
        diccionario_registros["VALOR_APORTE_DE_LA_ENTIDAD"] = registro[" Valor Aporte de la Entidad ($).1"]
        diccionario_registros["PORCENTAJE_APORTE_DE_LA_ENTIDAD"] = registro["Porcentaje Aporte de la Entidad (%)"]
        diccionario_registros["VALOR_APORTE_FONPACIFICO"] = registro["Valor Aporte de Fonpacífico ($)"]
        diccionario_registros["PORCENTAJE_APORTE_FONPACIFICO"] = registro["Porcentaje Aporte de Fonpacífico (%)"]
        diccionario_registros["ENTIDAD_DEL_DERIVADO"] = registro["Entidad del Derivado"]
        lista_registros.append(diccionario_registros)
        consecutivo+=1
    return sql, lista_registros
def sql_crear_tabla_facturacion_recaudos():
    sql = """CREATE TABLE IF NOT EXISTS apl1_facturacion_recaudos(ID INTEGER PRIMARY KEY
            ,   CODIGO_FACTURACION TEXT
            ,   FECHA_FACTURACION DATE
            ,   VALOR_FACTURACION INTEGER
            ,   VALOR_EN_SALDO INTEGER
            ,   VALOR_DE_ABONOS INTEGER
            ,   CODIGO_RECAUDO TEXT
            ,   FECHA_DE_RECAUDACION DATE	
            ,   VALOR_CONSIGNADO INTEGER
            ,   VALOR_DEDUCCIONES INTEGER
            ,   ENTIDAD_DEL_DERIVADO
            ,   ID_PROYECTO_CONTRATO_CONVENIO
            ,   PORC_5_SOBRE_TOTAL_REGALIAS INTEGER
            ,   PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE INTEGER
            ,   PORC_5_DEL_58_INTERADMINISTRATIVO INTEGER
            ,   VALOR_APORTADO_CONVENIO_POR_FONPACI INTEGER
            ,   POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS INTEGER
            )
        """
    return sql
def organizar_sql_insert_apl1_facturacion_recaudos(df_combinado_facturacion_recaudos):
    lista_registros = []
    diccionario_registros = {}
    consecutivo = 0
    sql = f"""INSERT INTO apl1_facturacion_recaudos(ID
        ,   CODIGO_FACTURACION
        ,   FECHA_FACTURACION
        ,   VALOR_FACTURACION
        ,   VALOR_EN_SALDO
        ,   VALOR_DE_ABONOS
        ,   CODIGO_RECAUDO
        ,   FECHA_DE_RECAUDACION
        ,   VALOR_CONSIGNADO
        ,   VALOR_DEDUCCIONES
        ,   ENTIDAD_DEL_DERIVADO
        ,   ID_PROYECTO_CONTRATO_CONVENIO
        ) VALUES(:ID
        ,   :CODIGO_FACTURACION
        ,   :FECHA_FACTURACION
        ,   :VALOR_FACTURACION
        ,   :VALOR_EN_SALDO
        ,   :VALOR_DE_ABONOS
        ,   :CODIGO_RECAUDO
        ,   :FECHA_DE_RECAUDACION	
        ,   :VALOR_CONSIGNADO
        ,   :VALOR_DEDUCCIONES
        ,   :ENTIDAD_DEL_DERIVADO
        ,   :ID_PROYECTO_CONTRATO_CONVENIO
        );
    """
    for registro in df_combinado_facturacion_recaudos.to_dict(orient='records'):
        FECHA_FACTURACION = registro["fecha_x"]
        FECHA_DE_RECAUDACION = registro["fecha_y"]
        diccionario_registros = {}
        diccionario_registros["ID"] = consecutivo
        diccionario_registros["CODIGO_FACTURACION"] = registro["codigo"]
        diccionario_registros["FECHA_FACTURACION"] = FECHA_FACTURACION
        diccionario_registros["VALOR_FACTURACION"] = registro["valor"]
        diccionario_registros["VALOR_EN_SALDO"] = registro["saldo"]
        diccionario_registros["VALOR_DE_ABONOS"] = registro["abonos"]
        diccionario_registros["CODIGO_RECAUDO"] = registro["factura"]
        diccionario_registros["FECHA_DE_RECAUDACION"] = FECHA_DE_RECAUDACION
        diccionario_registros["VALOR_CONSIGNADO"] = registro["consignacion"]
        diccionario_registros["VALOR_DEDUCCIONES"] = registro["deducciones"]
        diccionario_registros["ENTIDAD_DEL_DERIVADO"] = registro["Entidad del Derivado"]
        diccionario_registros["ID_PROYECTO_CONTRATO_CONVENIO"] = registro["N Proyecto / Contrato / Convenio / "]
        lista_registros.append(diccionario_registros)
        consecutivo+=1
    return sql, lista_registros
def sql_crear_tabla_ingresos_egresos_x_dia():
    sql = """CREATE TABLE IF NOT EXISTS apl1_ingresos_egresos_x_dia(ID INTEGER PRIMARY KEY
            ,   FECHA DATE
            ,   PORC_5_SOBRE_TOTAL_REGALIAS INTEGER
            ,   PORC_5_SOBRE_VALOR_CONVENIO_SIN_APORTE INTEGER
            ,   PORC_5_DEL_58_INTERADMINISTRATIVO INTEGER
            ,   VALOR_APORTADO_CONVENIO_POR_FONPACI INTEGER
            ,   POR_42_DEDUCIBLE_DE_INTERADMINSTRATIVOS INTEGER
            )
        """
    return sql
def sql_crear_tabla_flujos_caja():
    sql = """CREATE TABLE IF NOT EXISTS apl1_flujo_caja(ID INTEGER PRIMARY KEY
            ,   FECHA DATE
            ,   INGRESOS INTEGER
            ,   EGRESOS INTEGER
            )
        """
    return sql
def otras_consultas():
    sql = """
            select pr.ID_PROYECTO_CONTRATO_CONVENIO , fr.CODIGO_FACTURACION, pr.VALOR_APORTE_FONPACIFICO, fr.FECHA_DE_RECAUDACION
            from apl1_facturacion_recaudos  fr, apl1_proyectos pr
            where fr.ID_PROYECTO_CONTRATO_CONVENIO = pr.ID_PROYECTO_CONTRATO_CONVENIO
            and fr.ID_PROYECTO_CONTRATO_CONVENIO like '%CONVENIO%'
            order by fr.FECHA_DE_RECAUDACION desc;
        """