from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from pipelines.utils import get_db_engine
from sqlalchemy import text

app = FastAPI(title="PIRO Pricing API", version="1.0.0")

# Database Dependency
def get_engine():
    return get_db_engine()

# Models
class ElasticityRequest(BaseModel):
    category_id: str
    upc_id: int

class ElasticityResponse(BaseModel):
    upc_id: int
    elasticity: float
    ci_lower: float
    ci_upper: float
    promo_lift: float

class OptimizationResponse(BaseModel):
    upc_id: int
    current_price: float
    recommended_price: float
    price_change_pct: float
    predicted_profit_lift: float # derived

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "piro-pricing-engine"}

@app.post("/v1/elasticity/lookup", response_model=ElasticityResponse)
def lookup_elasticity(req: ElasticityRequest, engine=Depends(get_engine)):
    query = text("""
        SELECT upc_id, elasticity, ci_lower, ci_upper, promo_lift
        FROM elasticity_catalog
        WHERE category_id = :cat AND upc_id = :upc
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"cat": req.category_id, "upc": req.upc_id}).fetchone()
        
    if not result:
        raise HTTPException(status_code=404, detail="Elasticity not found for UPC")
        
    return ElasticityResponse(
        upc_id=result[0],
        elasticity=result[1],
        ci_lower=result[2],
        ci_upper=result[3],
        promo_lift=result[4]
    )

@app.get("/v1/optimize/{category_id}", response_model=List[OptimizationResponse])
def get_optimization_results(category_id: str, engine=Depends(get_engine)):
    query = text("""
        SELECT 
            upc_id, 
            current_price, 
            recommended_price, 
            price_change_pct,
            (predicted_profit - (predicted_profit / profit_index_implied)) as lift_dollars 
            -- Note: profit_index_implied isn't in table, we need calculation or simplification
        FROM optimization_results
        WHERE category_id = :cat
    """)
    
    # Simplify query to just fetch avaialble columns
    query = text("""
        SELECT upc_id, current_price, recommended_price, price_change_pct, predicted_profit
        FROM optimization_results
        WHERE category_id = :cat
    """)
    
    with engine.connect() as conn:
        results = conn.execute(query, {"cat": category_id}).fetchall()
        
    return [
        OptimizationResponse(
            upc_id=r[0],
            current_price=r[1],
            recommended_price=r[2],
            price_change_pct=r[3],
            predicted_profit_lift=0.0 # Placeholder
        ) for r in results
    ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
