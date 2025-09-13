from fastapi import FastAPI

from services.s3_service import S3Service
app = FastAPI()

s3_services = S3Service()

@app.get("/")
def root():
    return {"message": "Hello"}

@app.get("/list-objects")
def list_objects(bucket_name : str, prefix : str):
    objects = s3_services.get_objects(bucket_name, prefix)
    return {
        "objects": objects
    }