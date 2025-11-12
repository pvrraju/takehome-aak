from fastapi import FastAPI
import duckdb
from pathlib import Path


app = FastAPI()



current_dir = Path(__file__).parent

db_path = current_dir / "data" / "worldbank_poplulation_data.duckdb"



conn = duckdb.connect(str(db_path))


@app.get("/")

def home():
    return {"message": "Welcome to the World Bank Population Data API"}


@app.get("/population/{iso_code}")
def get_population(iso_code: str):

    iso_code = iso_code.upper()

    query = f"SELECT iso3, Official_Name, population, population_year, region_name, income_level FROM gold.population_latest WHERE iso3 = '{iso_code}'"

    try:
        result = conn.execute(query).df()


        if len(result) == 0:
            return {"error": " {iso_code} code not found"}
        
        row = result.iloc[0]

        return { 

            "iso_code" : row["iso3"],
            "country_name": row["Official_Name"],
            "population": row["population"],
            "population_year": row["population_year"],
            "region": row["region_name"],
            "income_level": row["income_level"]
        }
    
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/all")

def get_all():

    query = "SELECT iso3, Official_Name, population FROM gold.population_latest ORDER BY population DESC"

    result = conn.execute(query).df()

    countries = []


    for index, row in result.iterrows():
        countries.append({
            "iso_code": row["iso3"],
            "country_name": row["Official_Name"],
            "population": row["population"]
        })
    return {"countries": countries , "total" : len(countries)}