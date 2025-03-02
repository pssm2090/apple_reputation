import pandas as pd
from pymongo import MongoClient
import nest_asyncio
from fastapi import FastAPI
import uvicorn
import numpy as np

# Fix event loop issue in Jupyter
nest_asyncio.apply()

# MongoDB Cloud URI
MONGO_URI = "mongodb+srv://pssm2090:BRANDREPUTATIONCLUSTER@brandreputationcluster.ryyfq.mongodb.net/?retryWrites=true&w=majority&appName=BrandReputationCluster"

# Database and Collection Name
DB_NAME = "BrandReputationDB"
COLLECTION_NAME = "AppleReviews"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
print("✅ Connected to MongoDB Cloud!")

# Load CSV file into MongoDB
csv_file = "D:/Projects/LastYrrProject- BrandReputation/ABSA_Apple_good_bad.csv"
df = pd.read_csv(csv_file)
data = df.to_dict(orient="records")
collection.insert_many(data)
print("✅ CSV successfully loaded into MongoDB Cloud!")

# Initialize FastAPI
app = FastAPI()

# Function to handle NaN values
def replace_nan_with_none(data):
    if isinstance(data, dict):
        return {k: replace_nan_with_none(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_nan_with_none(v) for v in data]
    elif isinstance(data, float) and np.isnan(data):
        return None
    return data

@app.get("/")
def home():
    return {"message": "Welcome to Brand Reputation API 🚀"}

@app.get("/reviews")
def get_reviews():
    reviews = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB "_id"
    reviews = replace_nan_with_none(reviews)
    return {"reviews": reviews}

@app.get("/reviews/{rating}")
def get_reviews_by_rating(rating: int):
    reviews = list(collection.find({"rating": rating}, {"_id": 0}))
    reviews = replace_nan_with_none(reviews)
    return {"reviews": reviews}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
