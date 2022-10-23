import aiokafka
import asyncio
from mareeldatabase import createRawTable, createDurationTable, mareelDB



# ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:9000
async def consume(service, topic):
    mareeldb = mareelDB()

    consumer = aiokafka.AIOKafkaConsumer(topic, bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092')

    if service.split('_')[-1] == 'Raw':
        table = createRawTable(service)
    elif service.split('_')[-1] == 'Duration':
        table = createDurationTable(service)

    await consumer.start()
    try:
        while True:
            msg = await consumer.getone()
            data = table(msg.value)
            mareeldb.session.add(data)
            mareeldb.session.commit()

    except Exception as e:
        print(e)
    finally:
        await consumer.stop()



async def main():
    servers = [('Mareel_GO_Raw','South-Korea_146.56.145.179_raw'),
               ('Mareel_GO_Duration','South-Korea_146.56.145.179_duration'),
               ('Mareel_GO_Raw', 'Japan_141.147.190.169_raw'),
               ('Mareel_GO_Duration', 'Japan_141.147.190.169_duration')
               ]



    await asyncio.gather(*[consume(service, server) for service, server in servers])

if __name__ == "__main__":
    asyncio.run(main())