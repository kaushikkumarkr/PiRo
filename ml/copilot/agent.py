import sys
import argparse
from mlx_lm import load, generate
from sqlalchemy import create_engine, text
import pandas as pd

# Define Model Repo
MODEL_REPO = "mlx-community/Llama-3.2-3B-Instruct-4bit"

class CopilotAgent:
    def __init__(self, db_url="postgresql://admin:admin@localhost:5432/piro_db"):
        print(f"Loading Model: {MODEL_REPO}...")
        self.model, self.tokenizer = load(MODEL_REPO)
        self.engine = create_engine(db_url)
        print("Agent Initialized.")

    def get_pricing_context(self, category_id='sdr'):
        """
        Retrieves recent performance metrics to ground the LLM.
        """
        query = text(f"""
            SELECT 
                u.description,
                f.log_price,
                f.avg_price_4w,
                ec.elasticity
            FROM mart_weekly_pricing_features f
            JOIN dim_upc u USING(upc_id)
            LEFT JOIN elasticity_catalog ec USING(upc_id)
            WHERE u.category_id = '{category_id}'
            ORDER BY f.week_id DESC
            LIMIT 5
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        # Format as Context String
        context_str = "Recent Market Data:\n"
        for _, row in df.iterrows():
            context_str += (
                f"- Product: {row['description']}\n"
                f"  - Current Price (Log): {row['log_price'] if row['log_price'] else 0:.2f}\n"
                f"  - Avg Price 4w: {row['avg_price_4w'] if row['avg_price_4w'] else 0:.2f}\n"
                f"  - Elasticity: {row['elasticity'] if row['elasticity'] else 0:.2f}\n"
            )
        return context_str

    def reason(self, user_query, category_id='sdr'):
        """
        Generates a response using RAG.
        """
        print(f"User Query: {user_query}")
        
        # 1. Retrieve Context
        context = self.get_pricing_context(category_id)
        
        # 2. Construct Prompt
        # Llama-3 Instruct Format
        prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are PIRO, an expert Pricing AI. Use the provided market data to answer the merchant's question. 
Explain your reasoning clearly. If a product is highly elastic (elasticity < -1.5), suggest simpler price cuts. 
If inelasitc (> -1.0), suggest holding or raising price.

Context:
{context}
<|eot_id|><|start_header_id|>user<|end_header_id|>

{user_query}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
"""
        
        # 3. Generate
        print("Generating Response...")
        response = generate(
            self.model, 
            self.tokenizer, 
            prompt=prompt, 
            max_tokens=500, 
            verbose=True
        )
        return response

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, default="Why should we decrease the price of Pepsi?")
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    agent = CopilotAgent()
    agent.reason(args.query, args.category)
