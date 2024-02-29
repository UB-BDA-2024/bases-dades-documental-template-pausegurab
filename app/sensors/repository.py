from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo_db = MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    add_collection(mongo_db=mongo_db, sensor = sensor)
    return db_sensor

def add_collection(mongo_db: MongoDBClient, sensor: schemas.SensorCreate):
    info = mongo_db.getCollection("sensors")
    coll = {
        "name": sensor.name,
        "longitude": sensor.longitude,
        "latitude": sensor.latitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model" : sensor.model,
        "serie_number" : sensor.serie_number,
        "firmware_version" : sensor.firmware_version
    }
    info.insert_one(coll)
    

def record_data(sensor_id: int, db: Session, redis: RedisClient, data: schemas.SensorData) -> schemas.Sensor:
    # ldata = last_data.SensorData(temperature=data.temperature, humidity=data.humidity, battery_level=data.battery_level, last_seen=data.last_seen)
    
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(id) + ":temperatura"
    hum = "sensor" + str(id) + ":humidity"
    bat = "sensor" + str(id) + ":battery_level"
    seen = "sensor" + str(id) + ":last_seen"
    vel = "sensor" +str(id) +":velocity"

    # Fem els post de cada un dels atributs amb la seva clau i el seu valor
    redis.set(temp, data.temperature)
    redis.set(hum, data.humidity)
    redis.set(bat, data.battery_level)
    redis.set(seen, data.last_seen)
    redis.set(vel, data.velocity)

    
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


    

    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude=db_sensor.latitude,
        longitude=db_sensor.longitude,
        joined_at=str(db_sensor.joined_at),
        last_seen=data.last_seen,
        type=var,
        mac_adress=var,
        battery_level=data.battery_level,
        temperature=data.temperature,
        humidity=data.humidity,
        velocity=data.velocity
        
    )

def get_data(db: Session, sensor_id: int, redis: RedisClient) -> schemas.Sensor:
    # Trobem el sensor corresponent a la id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(id) + ":temperatura"
    hum = "sensor" + str(id) + ":humidity"
    bat = "sensor" + str(id) + ":battery_level"
    seen = "sensor" + str(id) + ":last_seen"
    vel = "sensor" + str(id) +":velocity"

    # Fem el return amb les dades corresponents del postgres i el redis
    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude=db_sensor.latitude,
        longitude=db_sensor.longitude,
        joined_at=str(db_sensor.joined_at),
        last_seen=redis.get(seen),
        type=var,
        mac_adress=var,
        battery_level=redis.get(bat),
        temperature=redis.get(temp),
        humidity=redis.get(hum),
        velocity=redis.get(vel)
        
    )

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor