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

    servers = [('Mareel_GO_Test_Raw', 'South-Korea_146.56.145.179_raw'),
               ('Mareel_GO_Test_Duration', 'South-Korea_146.56.145.179_duration'),
               ('Mareel_GO_Test_Raw', 'Japan_141.147.190.169_raw'),
               ('Mareel_GO_Test_Duration', 'Japan_141.147.190.169_duration'),

               ('Mareel_GO_Raw', 'India_144.24.119.251_raw'),
               ('Mareel_GO_Duration', 'India_144.24.119.251_duration'),
               ('Mareel_GO_Raw', 'Japan_54.250.113.35_raw'),
               ('Mareel_GO_Duration', 'Japan_54.250.113.35_duration'),
               ('Mareel_GO_Raw', 'South-Korea_146.56.42.103_raw'),
               ('Mareel_GO_Duration', 'South-Korea_146.56.42.103_duration'),
               ('Mareel_GO_Raw', 'United-States_129.158.221.8_raw'),
               ('Mareel_GO_Duration', 'United-States_129.158.221.8_duration'),]

    await asyncio.gather(*[consume(service, server) for service, server in servers])


if __name__ == "__main__":
    asyncio.run(main())
