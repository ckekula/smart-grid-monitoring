from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Welcome to your FastAPI application!"}

@app.get("/status")
def get_status():
    return {"status": "healthy"}
