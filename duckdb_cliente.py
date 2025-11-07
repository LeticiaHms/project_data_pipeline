import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime


class DuckDbClient:

    def __int__(self, db_path: str = "data/datalake.duckdb"):
        self.db_path = db_path
        Path("data").mkdir(exist_ok=True)
        self._initialize_database()
    
    def get_connection(self):
        return duckdb.connect(self.db_path)
    
    def _initialize_database(self):
        conn = self.get_connection()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_events(
                     id VARCHAR PRIMARY KEY
                     data JSON
                     ingestion_timestamp TIMESTAMP
                     source VARCHAR)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_sales(
                     id VARCHAR PRIMARY KEY
                     data JSON
                     ingestion_timestamp TIMESTAMP
                     source VARCHAR)
            )
        """)

        conn.close()

    def insert_bronze_data(self, table_name: str, records: List[Dict[str,Any]]) -> int:
        conn = self.get_connection()

        data_to_insert = []

        for record in records:
            record_id = (
                record.get('event_id') or
                record.get('sales_id') or
                f"{table_name}_{datetime.now().timestamp}"
            )
            data_to_insert.append({
                'id': record_id,
                'data' : json.dumps(record, default=str),
                'ingestion_timestamp' : datetime.now(),
                'source' : record.get('source','api')
            }

            )
        df = pd.DataFrame(data_to_insert)
        conn.execute(f"INSERT INTO bronze_{table_name} SELECT * FROM df")

        count = len(df)
        conn.close()

        return count
    
    def execute_query(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        result_df = conn.execute(query).fetchdf()
        conn.close()
        
        return result_df.to_dict(orient='records')
    
    def list_tables(self, layer: str = None) -> List[str]:
        conn = self.get_connection()
        
        if layer:
            query = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
                AND table_name LIKE '{layer}_%'
            """
        else:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """
        
        tables_df = conn.execute(query).fetchdf()
        conn.close()
        
        return tables_df['table_name'].tolist()
