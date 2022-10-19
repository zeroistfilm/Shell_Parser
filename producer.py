import asyncio

import cv2
import aiokafka
from fastapi import FastAPI
import websockets
import json

app = FastAPI()





async def producer():
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers='146.56.145.179:29092')
    await producer.start()
    data = 'test'
    try:
        await producer.send_and_wait(topic='test', value=data)
        await asyncio.sleep(1)
    finally:
        await producer.stop()


asyncio.create_task(producer())
