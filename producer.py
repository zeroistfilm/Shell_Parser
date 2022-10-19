import asyncio
import aiokafka
import datetime
#
# async def func(num):
#     print('Starting func {0}...'.format(num))
#     await asyncio.sleep(0.1)
#     print('Ending func {0}...'.format(num))
#
#
async def test():



    while True:
        producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092')
        await producer.start()
        data = b'hello kafka' + bytes(str(datetime.datetime.now())[:-7], encoding='utf-8')
        await producer.send_and_wait('test', data)
        print(f'send', data)
        await asyncio.sleep(0.5)
        await producer.stop()

asyncio.run(test())

