import traceback

import aiokafka
import asyncio
from mareeldatabase import createRawTable, createDurationTable, mareelDB, createPaymentTable


# http://ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:9000
async def consume(service, topic):
    while True:
        mareeldb = mareelDB()
        consumer = aiokafka.AIOKafkaConsumer(topic,
                                             bootstrap_servers=['127.0.0.1:29092',
                                                                '127.0.0.1:29093',
                                                                '127.0.0.1:29094'])

        if service.split('_')[-1] == 'Raw':
            table = createRawTable(service)
        elif service.split('_')[-1] == 'Duration':
            table = createDurationTable(service)
        elif service.split('_')[-1] == 'Payment':
            table = createPaymentTable(service)

        await consumer.start()
        try:
            while True:
                bulk = []

                msg = await consumer.getmany()

                for topic, messages in msg.items():
                    for message in messages:
                        print(len(bulk), message)

                        bulk.append(table(message.value))
                # print(len(bulk))
                await asyncio.sleep(0.01)
                # print(len(bulk),'================================================================================')

                mareeldb.session.add_all(bulk)
                mareeldb.session.commit()

        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("kafka consumer Error : ", message)

        finally:
            await consumer.stop()
            mareeldb.session.close()
            mareeldb.DATABASES.dispose()
async def main():
    # test
    while True:
        try:
            servers = [('Mareel_PRO_Raw', 'South-Korea_141.164.39.103_raw'),
                       ('Mareel_PRO_Duration', 'South-Korea_141.164.39.103_duration'),
                       ('Mareel_PRO_Raw', 'South-Korea_141.164.38.209_raw'),
                       ('Mareel_PRO_Duration', 'South-Korea_141.164.38.209_duration')]

            await asyncio.gather(*[consume(service, server) for service, server in servers])

        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("asyncio producer Error : ", message)

if __name__ == "__main__":
    asyncio.run(main())
