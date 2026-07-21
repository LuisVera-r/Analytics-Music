# Analytics-Music

# Stack

- Duckdb
- Faker
- python 
- apache airflow
- dbt
- dagster
- Sql
- [[Github Actions]]
- git
- faker

# Que hace 

Estoy desarrollando un proyecto end-to-end con el objetivo es simular el consumo de música en streaming a gran escala, aplicando técnicas avanzadas de modelado de datos como dimensiones lentamente cambiantes (SCD-2).

# Arquitectura del Proyecto

El pipeline está diseñado de forma modular para garantizar la escalabilidad y facilidad de mantenimiento:
* **Ingesta/Generación:** Módulos independientes en Python (`/data/generators`) encargados de simular datos realistas con integridad referencial, usando la libreria faker 
* **Almacenamiento:** Modelo de estrella (Star Schema) implementado en DuckDB.
* **Orquestación y CI/CD:** Automatización de pruebas a través de GitHub Actions.

## Modelo de Datos (Esquema en Estrella)
El Data Warehouse se compone de las siguientes tablas:
* **Tablas de Hechos:** `fact_streams` (Métricas de reproducción).
* **Tablas de Dimensiones:** `dim_user` (Manejo de histórico SCD-2 con 300 cambios en region), `dim_track`, `dim_artist`, `dim_date`.

# Fase 1 - [[Modelado dimensional|Star Schema]]


Primero cree el star schema con las dimension tables y su facts table usando duckdb por que cuenta con un motor olap, DuckDB puede consultar un DataFrame de Pandas o un archivo CSV directamente usando SQL, sin necesidad de cargar los datos formalmente primero. Es extremadamente eficiente con el manejo de la memoria RAM. y los guarde en una carpeta /schema donde separe cada tabla en un archivo .sql, porque permite la modularidad y cumple con uno de los pricipos solid

![[Star Schema.png]]

# Fase 2 - Ingesta e inserción de Datos usando faker

Creamos datos simulados usando la libreria faker para despues insertarlos en cada una de las tablas en duckdb esto busca simular la parte de extraccion de datos, cada generador de cada tabla esta separado en un archivo .py dentro de la carpeta generators

el la tabla dim_user tambien simule cambios para poder implemetar SCD-2 despuess 

ase 3 Implementación de Airflow

ya estando con la base de datos creada y almacenada en un star chema con:
- 1,000 Artistas
- Fechas de 4 años(365*4) desde 2022, 1, 1 hasta 2026, 1, 1
- 10,000 canciones 
- 5,000 usuarios - simula 300 cambios de region para los usuarios SCD-2
- 250,000 Streams 

Implementamos airflow para realizar la extracción de nuevos streams desde "2026-01-01 00:00:00" haasta 2026-01-08 con 50,000 streams entre estos dias 
