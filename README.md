# databricks_econ_medallion_project.

Databricks End-to-End ETL Medallion Architecture
Project Overview

This project demonstrates the design and implementation of an end-to-end ETL pipeline using Databricks Medallion Architecture. The solution ingests raw Orders and Products JSON data and processes it through the Bronze, Silver, and Gold layers to produce clean, transformed, and analytics-ready datasets.

The project uses Databricks Notebooks, PySpark, SQL, Delta Lake, Catalog, Jobs, and Pipeline Orchestration to automate data ingestion, transformation, and processing across the different layers.

Architecture



Source: Raw Orders and Products JSON files.
Bronze: Stores the raw ingested data with minimal transformation.
Silver: Cleans, validates, standardizes, and transforms the data.
Gold: Contains curated and analytics-ready datasets for reporting and business intelligence.


Key Technologies
Databricks
PySpark
SQL
Delta Lake
Medallion Architecture
Databricks Catalog
Databricks Jobs & Pipelines
JSON
ETL/ELT
Key Features

The pipeline demonstrates data ingestion, data cleansing, transformation, layer-to-layer processing, pipeline orchestration, and the creation of reliable analytics-ready datasets.




Project Objective

The objective is to build a scalable and reliable data engineering solution that transforms raw business data into high-quality datasets that can support analytics, reporting, and data-driven decision-making.



Outcome

The project provides practical experience in building modern Databricks Lakehouse ETL pipelines and applying the Medallion Architecture to organize, transform, and manage data efficiently from raw ingestion to business-ready information.

By Oladebo Ayanniyi 
Data Engineer
