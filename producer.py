import asyncio
import aiokafka


async def test():
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers='ec2-3-34-16-150.ap-northeast-2.compute.amazonaws.com:29092')
    await producer.start()
    data = 'test'
    while True:
        try:
            await producer.send_and_wait(topic='test', value=data)
            await asyncio.sleep(1)
        finally:
            await producer.stop()


asyncio.run(asyncio.create_task(test()))
