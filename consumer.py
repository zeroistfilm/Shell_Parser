import aiokafka
import asyncio

async def consume():

    consumer = aiokafka.AIOKafkaConsumer('test', bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com')
    await consumer.start()

    try:

            msg = await consumer.getone()
            #await websocket.send_text(msg.value)
            print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
            await asyncio.sleep(0.05)
    except Exception as e:
        print(e)
    finally:
        await consumer.stop()

asyncio.run(consume())