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

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo_db: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    add_document(mongo_db=mongo_db, sensor = sensor)
    return db_sensor

def get_sensors_near(db: Session, mongodb: MongoDBClient,latitude: float, longitude: float, radius: float):
    query = {
    'location': {
        '$near': {
            '$geometry': {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            },
            '$maxDistance': radius 
        }
    }
}
    
    database = mongodb.getDatabase("data")
    col = mongodb.getCollection("sensors")

    sensors_near = col.find(query)
    sensors_list = []

    for sensors in sensors_near:
        sensor_name = sensors["name"]
        db_sensor = get_sensor_by_name(sensor_name)
        sensors_list.append(db_sensor)

    return sensors_list



def add_document(mongo_db: MongoDBClient, sensor: schemas.SensorCreate):
    database = mongo_db.getDatabase("data")
    info = mongo_db.getCollection("sensors")
    

    if "location_2dsphere" not in info.index_information():
        info.create_index([("location", "2dsphere")])
    


    coll = {
        "name": sensor.name,
        "location" :{
                'type': 'Point',
                'coordinates': [sensor.longitude, sensor.latitude]
        }, 
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model" : sensor.model,
        "serie_number" : sensor.serie_number,
        "firmware_version" : sensor.firmware_version
    }

    info.insert_one(coll)
    

def record_data(sensor_id: int, db: Session, redis: RedisClient, data: schemas.SensorData, mongo_db: MongoDBClient) -> schemas.Sensor:
    
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(id) + ":temperatura"
    hum = "sensor" + str(id) + ":humidity"
    bat = "sensor" + str(id) + ":battery_level"
    seen = "sensor" + str(id) + ":last_seen"
    vel = "sensor" + str(id) + ":velocity"

    # Fem els post de cada un dels atributs amb la seva clau i el seu valor
   
    redis.set(bat, data.battery_level)
    redis.set(seen, data.last_seen)
    if data.velocity is not None:
        redis.set(vel, data.velocity)
    if data.temperature is not None:      
        redis.set(temp, data.temperature)
    if data.humidity is not None:
        redis.set(hum, data.humidity)

    
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    sensor_name = db_sensor.name

    mdb = mongo_db.getDatabase("data")
    col = mongo_db.getCollection("sensors")


    documental_sensor = col.find_one({"name": sensor_name})

    
    last_seen = redis.get(seen)
    battery_level = redis.get(bat)
    temperature = None
    humidity = None
    velocity = None
    if data.temperature is not None:
        temperature = redis.get(temp)
    if data.humidity is not None:
        humidity = redis.get(hum)
    if data.velocity is not None:
        velocity = redis.get(vel)


    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude=documental_sensor["location"]["coordinates"][1],
        longitude=documental_sensor["location"]["coordinates"][0],
        joined_at=str(db_sensor.joined_at),
        last_seen=last_seen,
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        temperature=temperature,
        humidity=humidity,
        velocity=velocity
        
    )

def get_data(db: Session, sensor_id: int, redis: RedisClient, mongo_db: MongoDBClient) -> schemas.Sensor:
    # Trobem el sensor corresponent a la id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(id) + ":temperatura"
    hum = "sensor" + str(id) + ":humidity"
    bat = "sensor" + str(id) + ":battery_level"
    seen = "sensor" + str(id) + ":last_seen"
    vel = "sensor" + str(id) + ":velocity"

    sensor_name = db_sensor.name

    # Fem el return amb les dades corresponents del postgres i el redis


    mdb = mongo_db.getDatabase("data")
    col = mongo_db.getCollection("sensors")
    
    documental_sensor = col.find_one({"name": sensor_name})

    last_seen = redis.get(seen)
    battery_level = redis.get(bat)
    temperature = redis.get(temp)
    humidity = redis.get(hum)
    velocity = redis.get(vel)

    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude=documental_sensor["location"]["coordinates"][1],
        longitude=documental_sensor["location"]["coordinates"][0],
        joined_at=str(db_sensor.joined_at),
        last_seen=last_seen,
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        temperature=temperature,
        humidity=humidity,
        velocity=velocity
        
    )

def delete_sensor(db: Session, sensor_id: int, mongo_db : MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    sensor_name = db_sensor.name
    database = mongo_db.getDatabase("data")
    col = mongo_db.getCollection("sensors")

    col.delete_one({"name": sensor_name})

    temp = "sensor" + str(id) + ":temperatura"
    hum = "sensor" + str(id) + ":humidity"
    bat = "sensor" + str(id) + ":battery_level"
    seen = "sensor" + str(id) + ":last_seen"
    vel = "sensor" + str(id) + ":velocity"

    redis.delete(temp)
    redis.delete(hum)
    redis.delete(bat)
    redis.delete(seen)
    redis.delete(vel)


    db.delete(db_sensor)
    db.commit()