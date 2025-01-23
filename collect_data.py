import requests
import polars as pl
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
if not GITHUB_TOKEN:
    raise ValueError("Le token GitHub n'est pas défini dans le fichier .env")

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
BASE_URL = "https://api.github.com/repos/jupyter/notebook"

def fetch_github_data(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def collect_data():
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.star+json"
    }
    
    stars = requests.get(f"{BASE_URL}/stargazers", headers=headers).json()
    stars_data = [{"user": star["user"]["login"], "starred_at": star["starred_at"]} for star in stars]

    forks = fetch_github_data("forks")
    forks_data = [{"user": fork["owner"]["login"], "created_at": fork["created_at"]} for fork in forks]

    issues = fetch_github_data("issues")
    issues_data = [{"id": issue["id"], "title": issue["title"], "state": issue["state"], 
                    "created_at": issue["created_at"], "closed_at": issue.get("closed_at")} for issue in issues]

    prs = fetch_github_data("pulls")
    prs_data = [{"id": pr["id"], "title": pr["title"], "state": pr["state"], 
                 "created_at": pr["created_at"], "merged_at": pr.get("merged_at")} for pr in prs]


    dataframes = {
        "stars": pl.DataFrame(stars_data),
        "forks": pl.DataFrame(forks_data),
        "issues": pl.DataFrame(issues_data),
        "pull_requests": pl.DataFrame(prs_data),
    }

    for name, df in dataframes.items():
        file_name = f"data/{name}.parquet"
        df.write_parquet(file_name)
        print(f"{file_name} enregistré avec succès.")

if __name__ == "__main__":
    collect_data()
