# PIRO: Price Intelligence & Revenue Optimization Platform

**Status**: Completed (Sprint 6)

PIRO is an industry-grade pricing engine leveraging the **Dominick's Finer Foods (DFF)** dataset (provided by the Kilts Center for Marketing). It implements a modern Lakehouse architecture with Bayesian Elasticity Modeling, Probabilistic Forecasting, and Constraint-Based Optimization to deliver actionable pricing recommendations.

---

## 1. Executive Summary

Retail pricing is often driven by intuition rather than data. PIRO solves this by scientifically measuring price sensitivity (elasticity) and simulating outcomes to optimize revenue and profit.

**Key Capabilities**:
- **Pricing Engine**: Hierarchical Bayesian models estimate elasticity at the Store/UPC level.
- **Forecasting**: Probabilistic demand forecasting using AutoARIMA.
- **Optimization**: Mixed-Integer Programming (MIP) to find optimal price points under global business constraints.
- **Simulation**: "What-if" analysis for price changes (e.g., impact of a 10% hike).

---

## 2. Quantitative Results

The platform has been trained and validated on the **Soft Drinks (SDR)** category.

### 📊 Model Performance
| Metric | Value | Interpretation |
| :--- | :--- | :--- |
| **Average Elasticity** | **-1.17** | Demand is elastic; a 10% price drop typically yields >11.7% volume lift. |
| **Promo Lift** | **+224%** | Promotions are highly effective, tripling baseline volume on average. |
| **Profit Optimization** | **+7.2%** | The optimizer identified a potential **7.2% increase in category profit** while maintaining 95% revenue. |

> *Note: Metrics derived from the `elasticity_catalog` and `optimization_results` tables for the SDR category.*

---

## 3. System Architecture

The system follows a modular "Lakehouse" design, separating data engineering, science, and serving.

```mermaid
graph TD
    subgraph "Ingestion & Warehouse"
        I[Ingest Scripts] -->|Pandas| DB[(Postgres DWH)]
        DB -->|dbt| M[Marts / Features]
    end

    subgraph "Science Engine"
        M -->|Read| E[Elasticity Model (PyMC)]
        M -->|Read| F[Forecasting (StatsForecast)]
        E -->|Coefficients| S[Simulator]
        S -->|Scenarios| O[Optimizer (OR-Tools)]
    end

    subgraph "Serving & UX"
        O -->|Results| API[FastAPI Service]
        M & O -->|BI| D[Metabase Dashboards]
    end
```

### Component Breakdown
1.  **Ingestion Layer**: Python pipelines load raw Stata/CSV files into Postgres `raw` schema.
2.  **Transformation Layer (dbt)**: 
    -   Cleans and normalizes data into a Star Schema (`dim_upc`, `fact_movement`).
    -   Generates ML features (`log_price`, `lag_sales`) in `marts`.
3.  **Science Engine**:
    -   **Elasticity**: PyMC Hierarchical Log-Log Model.
    -   **Forecasting**: StatsForecast (AutoARIMA) for baseline demand.
    -   **Optimization**: Google OR-Tools (SCIP Solver) for constrained optimization.
4.  **Serving Layer**: FastAPI provides REST endpoints for real-time integration.

---

## 4. Setup & Usage

### Prerequisites
-   Docker & Docker Compose
-   Python 3.9+ (if running locally without Docker)
-   DFF Dataset (Stata/CSV files) placed in `data/raw/`

### Quick Start
1.  **Start Services**:
    ```bash
    make up
    # Starts Postgres (5432), Metabase (3000), MLflow (5001), Minio (9000)
    ```

2.  **Run Data Pipeline**:
    ```bash
    make all
    # Ingests data -> Runs dbt transformations -> Runs Tests
    ```

3.  **Run Science Engine**:
    ```bash
    # Train Elasticity Model
    docker compose exec runner python ml/elasticity/train_model.py --category sdr

    # Generate Forecasts & Simulation
    docker compose exec runner python ml/forecasting/train_forecast.py --category sdr
    docker compose exec runner python ml/simulation/scenario_engine.py --category sdr
    ```

4.  **Run Optimization**:
    ```bash
    # Find profit-maximizing prices
    docker compose exec runner python ml/optimization/optimize_profit.py --category sdr
    ```

5.  **Start API**:
    ```bash
    docker compose exec -d runner uvicorn api.main:app --host 0.0.0.0 --port 8000
    ```

### API Endpoints
-   `GET /health`: System check.
-   `POST /v1/elasticity/lookup`: Get elasticity for a specific UPC/Store.
-   `GET /v1/optimize/sdr`: Get list of recommended price changes.

---

## 5. Technology Stack

-   **Language**: Python 3.9
-   **Database**: PostgreSQL 15
-   **Transformation**: dbt Core 1.7
-   **Machine Learning**:
    -   **PyMC**: Bayesian Inference.
    -   **StatsForecast**: Efficient Time Series Forecasting.
    -   **OR-Tools**: Operations Research / Optimization.
-   **Serving**: FastAPI, Uvicorn.
-   **Infrastructure**: Docker, Docker Compose, MinIO (S3-compatible storage).
