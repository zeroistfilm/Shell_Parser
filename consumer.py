import traceback

import aiokafka
import asyncio
from mareeldatabase import createRawTable, createDurationTable, createPaymentTable, mareelDB


# http://ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:9000
async def consume(service, topic, queue):
    print("start consum", topic)
    await asyncio.sleep(2)
    while True:
        consumer = aiokafka.AIOKafkaConsumer(topic,
                                             bootstrap_servers=['146.56.42.103:29092',
                                                                '146.56.42.103:29093',
                                                                '146.56.42.103:29094'])

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

                await queue.put(bulk)
                await asyncio.sleep(0.01)

        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("kafka consumer Error : ", message)
            await asyncio.sleep(2)

        finally:
            await consumer.stop()


async def saver(queue):
    print("start saver")
    global mareeldb
    mareeldb = mareelDB(useLocalDB=True)
    while True:
        try:
            bulk = await queue.get()
            mareeldb.session.add_all(bulk)
            mareeldb.session.commit()
        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("saver Error : ", message)
            await asyncio.sleep(2)
        finally:

            mareeldb.session.close()
            mareeldb.DATABASES.dispose()


async def main():
    print("start consumer")
    messageQueue = asyncio.Queue()
    while True:
        await asyncio.sleep(2)
        try:
            servers = [
                # GO
                ('Mareel_GO_Test_Raw', 'South-Korea_146.56.145.179_raw'),
                ('Mareel_GO_Test_Duration', 'South-Korea_146.56.145.179_duration'),
                ('Mareel_GO_Test_Payment', 'South-Korea_146.56.145.179_payment'),

                ('Mareel_GO_Test_Raw', 'Japan_141.147.190.169_raw'),
                ('Mareel_GO_Test_Duration', 'Japan_141.147.190.169_duration'),
                ('Mareel_GO_Test_Payment', 'Japan_141.147.190.169_payment'),

                ('Mareel_GO_Raw', 'India_144.24.119.251_raw'),
                ('Mareel_GO_Duration', 'India_144.24.119.251_duration'),
                ('Mareel_GO_Payment', 'India_144.24.119.251_payment'),

                ('Mareel_GO_Raw', 'Japan_54.95.222.100_raw'),
                ('Mareel_GO_Duration', 'Japan_54.95.222.100_duration'),
                ('Mareel_GO_Payment', 'Japan_54.95.222.100_payment'),

                ('Mareel_GO_Raw', 'South-Korea_146.56.42.103_raw'),
                ('Mareel_GO_Duration', 'South-Korea_146.56.42.103_duration'),
                ('Mareel_GO_Payment', 'South-Korea_146.56.42.103_payment'),

                ('Mareel_GO_Raw', 'United-States_129.158.221.8_raw'),
                ('Mareel_GO_Duration', 'United-States_129.158.221.8_duration'),
                ('Mareel_GO_Payment', 'United-States_129.158.221.8_payment'),

                ('Mareel_GO_Raw', 'South-Korea_152.70.249.8_raw'),
                ('Mareel_GO_Duration', 'South-Korea_152.70.249.8_duration'),
                ('Mareel_GO_Payment', 'South-Korea_152.70.249.8_payment'),

                # Pro
                #('Mareel_PRO_Raw', 'South-Korea_141.164.38.209_raw'),
                #('Mareel_PRO_Duration', 'South-Korea_141.164.38.209_duration')

                #VPN
                ('Mareel_VPN_Raw', 'India_129.154.233.34_raw'),
                ('Mareel_VPN_Duration', 'India_129.154.233.34_duration'),
                ('Mareel_VPN_Raw', 'Germany_45.77.65.232_raw'),
                ('Mareel_VPN_Duration', 'Germany_45.77.65.232_duration'),
                ('Mareel_VPN_Raw', 'Japan_108.61.182.243_raw'),
                ('Mareel_VPN_Duration', 'Japan_108.61.182.243_duration'),
                ('Mareel_VPN_Raw', 'South-Korea_141.164.53.7_raw'),
                ('Mareel_VPN_Duration', 'South-Korea_141.164.53.7_duration'),

                ('Mareel_VPN_Raw', 'United-States_129.159.126.80_raw'),
                ('Mareel_VPN_Duration', 'United-States_129.159.126.80_duration'),
                ('Mareel_VPN_Raw', 'United-States_54.200.124.241_raw'),
                ('Mareel_VPN_Duration', 'United-States_54.200.124.241_duration'),

                ('Mareel_VPN_Raw', 'South-Korea_152.70.236.45_raw'),
                ('Mareel_VPN_Duration', 'South-Korea_152.70.236.45_duration'),

            ]

            # print(*[saver(messageQueue), *[consume(service, server, messageQueue) for service, server in servers]])
            await asyncio.gather(
                *[saver(messageQueue), *[consume(service, server, messageQueue) for service, server in servers]])
        except Exception as e:
            await asyncio.sleep(2)
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("asyncio producer Error : ", message)


if __name__ == "__main__":
    asyncio.run(main())
