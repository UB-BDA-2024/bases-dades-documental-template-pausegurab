from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from . import models, schemas, repository


# Dependency to get db session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client
def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    try:
        yield mongodb
    finally:
        mongodb.close()


router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float,radius: float, db: Session = Depends(get_db),mongodb_client: MongoDBClient = Depends(get_mongodb_client), redis: RedisClient = Depends(get_redis_client)):
    #raise HTTPException(status_code=404, detail="Not implemented")
    return repository.get_sensors_near(db=db, mongodb=mongodb_client, redis=redis,latitude=latitude, longitude=longitude,radius=radius)


# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(status_code=400, detail="Sensor with same name already registered")
    return repository.create_sensor(db=db, sensor=sensor, mongo_db=mongodb_client)

# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), redis: RedisClient = Depends(get_redis_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.delete_sensor(db=db, sensor_id=sensor_id, mongo_db = mongodb_client, redis=redis)
    

# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData,db: Session = Depends(get_db) ,redis_client: RedisClient = Depends(get_redis_client), mongo_db: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id) 
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.record_data(sensor_id=sensor_id, db=db, redis=redis_client, data=data, mongo_db = mongo_db)


# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int, db: Session = Depends(get_db) ,redis_client: RedisClient = Depends(get_redis_client), mongodb_client : MongoDBClient = Depends(get_mongodb_client)):    
    db_sensor = repository.get_sensor(db, sensor_id) 
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.get_data(db=db, sensor_id=sensor_id, redis=redis_client, mongo_db=mongodb_client)
