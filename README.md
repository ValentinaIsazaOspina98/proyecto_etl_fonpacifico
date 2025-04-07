# PROYECTO ETL PARA FONPACIFICO

## Descripción General
Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) modular, orientado a fortalecer la planeación, el seguimiento financiero y la generación de reportes institucionales en Fonpacifico. La arquitectura se compone de módulos especializados que permiten la integración automatizada de datos provenientes de archivos planos generados por sistemas como SPGR y SOUL, facilitando su procesamiento, consolidación y uso en procesos contables, presupuestales y administrativos.

## Estructura de Módulos

### 1. extraccion.py
Módulo encargado de recolectar información desde archivos `.csv` delimitados por `;`, estructurados por origen:
- extraer_datos_matrix_proyectos
- extraer_datos_spgr_flujos
- extraer_datos_spgr_pagos
- extraer_datos_soul_recaudos
- extraer_datos_soul_facturacion

### 2. transformacion.py
Se encarga de procesar y transformar los datos:
- Filtrado: elimina columnas innecesarias, filtra por año, estado del proyecto, etc.
- Limpieza: formatea columnas monetarias y numéricas.
- Combinación: integra información de facturación, recaudo y proyectos.
- Cálculos y Carga: calcula indicadores clave y carga los resultados en la base de datos.

### 3. funciones.py
Módulo auxiliar para exportación, estructuración y visualización:
- Exporta a .csv y .xlsx (guardar_csv_datos, guardar_excel_datos).
- Estructura reportes normalizados (guardar_data_frame_a_csv_datos).
- Incluye funciones de impresión y visualización (mostrar_datos_database, imprimir_datos_dataframe).

### 4. pruebas.py
Proporciona funciones para validar individualmente cada componente durante el desarrollo, sin afectar el entorno de producción.

### 5. main.py
Archivo orquestador que coordina la ejecución completa del pipeline:
- Define rutas de entrada.
- Ejecuta los procesos en orden: extracción → transformación → carga/exportación.
- Calcula indicadores: 5% sobre regalías y convenios.
- Facilita la automatización del pipeline.

## IMPORTANTE: se debe modificar la ruta de los archivos de entrada y la base de datos directamente en el archivo main.py antes de ejecutar el pipeline, adaptándolas al entorno local o servidor correspondiente.

## Fuentes de Datos
- SPGR: Cronogramas de flujo de caja y pagos.
- SOUL: Ingresos reportados y facturación emitida.
- Matriz de proyectos institucionales: Información base para clasificaciones y seguimiento.

## Transformaciones Principales
- Limpieza de datos numéricos y monetarios.
- Filtrado por atributos relevantes.
- Estandarización de columnas y nombres.
- Cálculo de indicadores financieros como:
- 5% sobre total regalías.
- 5% sobre valor de convenios sin aporte.
- Combinación entre múltiples fuentes para consolidación.

## Destino de los Datos
- Base de datos local (SQLite por defecto) para persistencia estructurada.
- Archivos .csv y .xlsx para distribución y uso institucional.

## Control de Calidad
- Pruebas unitarias en pruebas.py para asegurar la confiabilidad de funciones críticas.
- Limpieza automática de formatos no estándares.
- Visualización directa de resultados y registros en consola para validación manual.

## Requerimientos del Proyecto - Tecnologías
- Python 3.10 o superior
- Bibliotecas:
- pandas
- openpyxl
- sqlalchemy
- sqlite3 (nativa)

## Instalación de dependencias:
pip install pandas openpyxl sqlalchemy

## Estructura de Carpetas
Fonpacifico_ETL/
├── extraccion.py
├── transformacion.py
├── funciones.py
├── pruebas.py
├── main.py
├── data/
│   ├── soul_facturacion.csv
│   ├── soul_recaudos.csv
│   ├── spgr_flujos.csv
│   ├── spgr_pagos.csv
│   └── matriz_proyectos.csv

## Ejecución
Desde terminal o entorno de desarrollo (VS Code, Jupyter, etc.):
python main.py
Esto activará todo el pipeline, desde la extracción hasta la exportación final de resultados.

## Visualización de Resultados
Los resultados consolidados permiten observar:
- Desfase entre ingresos y egresos a lo largo del tiempo.
- Análisis de picos atípicos de egresos (por ejemplo, mayo y julio de 2024).
- Impacto operativo generado por la desalineación financiera.
- Proyecciones e indicadores para planificación y toma de decisiones.
