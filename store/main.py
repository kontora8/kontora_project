import asyncio
import json
from typing import Set, Dict, List, Any

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, delete, update
from datetime import datetime
from pydantic import BaseModel, field_validator
from pydantic.json import pydantic_encoder
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)
import models
from models.modelsDB import ProcessedAgentDataInDB
from models.modelsFastAPI import ProcessedAgentData
import random

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("air", Integer),
    Column("noise", Integer),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)
metadata.create_all(engine)

# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

# FastAPI WebSocket endpoint
import random


@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)

    try:
        while True:
            with SessionLocal() as session:
                query = select(
                    processed_agent_data.c.latitude,
                    processed_agent_data.c.longitude,
                    processed_agent_data.c.road_state,
                    processed_agent_data.c.air,
                    processed_agent_data.c.noise,
                ).order_by(processed_agent_data.c.timestamp.desc()).limit(1)

                result = session.execute(query).fetchone()
            if result:
                latitude, longitude, road_state, air, noise = result
                if road_state is None:
                    road_state = "normal"

                data = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "road_state": road_state,
                    "air": air,
                    "noise": noise,
                }
                await send_data_to_subscribers(data)
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        subscriptions.remove(websocket)


async def send_data_to_subscribers(data):
    """Sending data to all subscribed clients"""
    disconnected_clients = []
    for websocket in subscriptions:
        try:
            await websocket.send_json(data)
        except WebSocketDisconnect:
            disconnected_clients.append(websocket)

    for websocket in disconnected_clients:
        subscriptions.remove(websocket)


# FastAPI CRUDL endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    print("Creating processed agent data...")

    with SessionLocal() as session:
        for item in data:
            query = processed_agent_data.insert().values(
                road_state=item.road_state,
                x=item.agent_data.accelerometer.x,
                y=item.agent_data.accelerometer.y,
                z=item.agent_data.accelerometer.z,
                air=item.agent_data.accelerometer.air,
                noise=item.agent_data.accelerometer.noise,
                latitude=item.agent_data.gps.latitude,
                longitude=item.agent_data.gps.longitude,
                timestamp=item.agent_data.timestamp,
            )
            session.execute(query)

        session.commit()
        print("Processed agent data was created!")

    # Send data to subscribers
    await send_data_to_subscribers(data)

@app.get("/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    print("Reading processed agent data by id...")

    with SessionLocal() as session:
        # print(processed_agent_data_id)
        query = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id
        )

        result = session.execute(query).first()
        print("Result: ", result)

        if result is None:
            print("Data not found")
            raise HTTPException(status_code=404, detail="Data not found")

        return result

@app.get("/processed_agent_data/",
    response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    # Get list of data
    print("Listing processed agent data...")

    with SessionLocal() as session:
        query = select(processed_agent_data)

        result = session.execute(query)
        print("Result: ", result)

        if result is None:
            print("Data not found")
            raise HTTPException(status_code=404, detail="Data not found")

        return result

@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    print("Updating processed agent data by id...")

    with SessionLocal() as session:
        query = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id)

        result = session.execute(query).first()
        print("Result: ", result)

        if result is None:
            print("Data not found")
            raise HTTPException(status_code=404, detail="Data not found")

        query = update(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id
        ).values(
            road_state=data.road_state,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp,
        )

        session.execute(query)
        session.commit()

        query = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id)
        result = session.execute(query).first()
        print("Result: ", result)

        return result


@app.delete("/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    print("Deleting processed_agent_data by id...")

    with SessionLocal() as session:
        query = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id)
        result = session.execute(query).first()
        print("Result: ", result)

        if result is None:
            print("Data not found")
            raise HTTPException(status_code=404, detail="Data not found")

        query = delete(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id
        )

        session.execute(query)
        session.commit()
        print(f"{processed_agent_data_id} was deleted!")
        return result

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)