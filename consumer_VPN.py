import aiokafka
import asyncio
from mareeldatabase import createRawTable, createDurationTable, mareelDB


# http://ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:9000
async def consume(service, topic):
    mareeldb = mareelDB()

    consumer = aiokafka.AIOKafkaConsumer(topic,
                                         bootstrap_servers=['ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092',
                                                            'ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29093',
                                                            'ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29094'])

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

    servers = [

               ('Mareel_VPN_Raw', 'Germany_52.29.224.219_raw'),
               ('Mareel_VPN_Duration', 'Germany_52.29.224.219_duration'),
               ('Mareel_VPN_Raw', 'Japan_35.79.143.27_raw'),
               ('Mareel_VPN_Duration', 'Japan_35.79.143.27_duration'),
               ('Mareel_VPN_Raw', 'Singapore_207.148.124.7_raw'),
               ('Mareel_VPN_Duration', 'Singapore_207.148.124.7_duration'),
        ('Mareel_VPN_Raw', 'South-Korea_130.162.152.89_raw'),
        ('Mareel_VPN_Duration', 'South-Korea_130.162.152.89_duration'),

               ('Mareel_VPN_Raw', 'South-Korea_172.107.194.178_raw'),
               ('Mareel_VPN_Duration', 'South-Korea_172.107.194.178_duration'),
               ('Mareel_VPN_Raw', 'United-Kingdom_104.238.184.85_raw'),
               ('Mareel_VPN_Duration', 'United-Kingdom_104.238.184.85_duration'),
               ('Mareel_VPN_Raw', 'United-States_129.159.126.80_raw'),
               ('Mareel_VPN_Duration', 'United-States_129.159.126.80_duration'),
               ('Mareel_VPN_Raw', 'United-States_54.200.124.241_raw'),
               ('Mareel_VPN_Duration', 'United-States_54.200.124.241_duration')]

    await asyncio.gather(*[consume(service, server) for service, server in servers])


if __name__ == "__main__":
    asyncio.run(main())
