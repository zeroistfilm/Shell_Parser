
import aiokafka
import asyncio
from mareeldatabase import createRawTable, createDurationTable, mareelDB


# http://ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:9000
async def consume(service, topic):
    mareeldb = mareelDB()

    consumer = aiokafka.AIOKafkaConsumer(topic,
                                         bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092')

    if service.split('_')[-1] == 'Raw':
        table = createRawTable(service)
    elif service.split('_')[-1] == 'Duration':
        table = createDurationTable(service)

    await consumer.start()
    try:
        while True:
            bulk = []

            msg = await consumer.getmany()

            for topic, messages in msg.items():
                for message in messages:
                    print(message)

                    bulk.append(table(message.value))
            # print(len(bulk))
            await asyncio.sleep(2)
            #print(len(bulk),'================================================================================')

            mareeldb.session.add_all(bulk)
            mareeldb.session.commit()

    except Exception as e:
        print(e)
    finally:
        await consumer.stop()


async def main():
    # test

    servers = [('Mareel_PRO_Raw', 'South-Korea_141.164.39.103_raw'),
               ('Mareel_PRO_Duration', 'South-Korea_141.164.39.103_duration'),
               ('Mareel_PRO_Raw', 'South-Korea_141.164.38.209_raw'),
               ('Mareel_PRO_Duration', 'South-Korea_141.164.38.209_duration')]

    await asyncio.gather(*[consume(service, server) for service, server in servers])


if __name__ == "__main__":
    asyncio.run(main())
