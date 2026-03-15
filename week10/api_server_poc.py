import uvicorn
from fastapi import FastAPI
from week10.utilities.logic import get_names_of_seed_lists

app = FastAPI()

@app.get("/")
async def welcome():
    return {"data": "welcome, frodo of the shire"}

@app.get("/seed_list_names")
async def seed_list_names_api():
    return {"data": get_names_of_seed_lists()}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
