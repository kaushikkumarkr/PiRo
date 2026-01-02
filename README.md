# PIRO: Price Intelligence & Revenue Optimization Platform

**Status**: Completed (Sprint 6)

PIRO is an industry-grade pricing engine leveraging the Dominick's Finer Foods (DFF) dataset. It implements a modern Lakehouse architecture with Bayesian Elasticity Modeling, Probabilistic Forecasting, and Constraint-Based Optimization.

## Key Features
- **Data Warehouse**: Star-schema Postgres DWH built with dbt.
- **Elasticity Engine**: Hierarchical Bayesian Model (PyMC) estimating price sensitivity at the UPC/Store level.
- **Forecasting**: Probabilistic baseline forecasts using AutoARIMA (StatsForecast).
- **Optimization**: Profit maximization using OR-Tools (MIP) with global revenue constraints.
- **REST API**: FastAPI service for real-time inference and recommendations.

## Setup Instructions

### 1. Place Data Files
Ensure the following files are in `piro-dff-pricing-platform/data/raw/`:
- `ccount(stata).zip`
- `demo(stata).zip`
- `upcsdr.csv`
- `wsdr.csv`

### 2. Start the Stack
Run the full stack (Postgres, Metabase, MLflow, Runner):
```bash
make up
# OR
docker-compose up -d --build
```

### 3. Run Pipeline (Ingestion + dbt)
To ingest data and build the staging models:
```bash
make all
```

## Running the Science Engine

### 1. Train Elasticity Model
```bash
docker compose exec runner python ml/elasticity/train_model.py --category sdr
```

### 2. Generate Forecasts & Simulation
```bash
docker compose exec runner python ml/forecasting/train_forecast.py --category sdr
docker compose exec runner python ml/simulation/scenario_engine.py --category sdr
```

### 3. Run Profit Optimization
```bash
# Finds optimal prices subject to 95% revenue constraint
docker compose exec runner python ml/optimization/optimize_profit.py --category sdr
```

### 4. Start the API
```bash
docker compose exec -d runner uvicorn api.main:app --host 0.0.0.0 --port 8000
```
**Endpoints**:
- `GET /health`
- `GET /v1/optimize/sdr`
- `POST /v1/elasticity/lookup`

## Services
- **Postgres**: localhost:5432
- **Metabase**: http://localhost:3000
- **MLflow**: http://localhost:5001
- **API**: http://localhost:8000

## Architecture
- **Ingestion**: Python scripts (pandas) loading Stata/CSV to Postgres.
- **Transformation**: dbt Core (SQL) for data modeling.
- **Science**: PyMC (Bayesian), StatsForecast (Time Series), OR-Tools (Optimization).
- **Serving**: FastAPI.

## Academic Use Acknowledgment
This project uses the Dominick's Finer Foods dataset provided by the Kilts Center for Marketing at the University of Chicago Booth School of Business.
