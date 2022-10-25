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
            msg = await consumer.getone()
            data = table(msg.value)
            mareeldb.session.add(data)
            mareeldb.session.commit()
            await asyncio.sleep(0.1)
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
               ('Mareel_GO_Duration', 'United-States_129.158.221.8_duration'),

               ('Mareel_PRO_Raw', 'South-Korea_141.164.39.103_raw'),
               ('Mareel_PRO_Duration', 'South-Korea_141.164.39.103_duration'),
               ('Mareel_PRO_Raw', 'South-Korea_141.164.38.209_raw'),
               ('Mareel_PRO_Duration', 'South-Korea_141.164.38.209_duration'),

               ('Mareel_VPN_Raw', 'Germany_45.77.65.232_raw'),
               ('Mareel_VPN_Duration', 'Germany_45.77.65.232_duration'),
               ('Mareel_VPN_Raw', 'India_15.206.180.184_raw'),
               ('Mareel_VPN_Duration', 'India_15.206.180.184_duration'),
               ('Mareel_VPN_Raw', 'Japan_35.79.143.27_raw'),
               ('Mareel_VPN_Duration', 'Japan_35.79.143.27_duration'),
               ('Mareel_VPN_Raw', 'Singapore_207.148.124.7_raw'),
               ('Mareel_VPN_Duration', 'Singapore_207.148.124.7_duration'),
               ('Mareel_VPN_Raw', 'South-Korea_172.107.194.178_raw'),
               ('Mareel_VPN_Duration', 'South-Korea_172.107.194.178_duration'),
               ('Mareel_VPN_Raw', 'United-Kingdom_104.238.184.85_raw'),
               ('Mareel_VPN_Duration', 'United-Kingdom_104.238.184.85_duration'),
               ('Mareel_VPN_Raw', 'United-States_104.156.250.10_raw'),
               ('Mareel_VPN_Duration', 'United-States_104.156.250.10_duration'),
               ('Mareel_VPN_Raw', 'United-States_54.200.124.241_raw'),
               ('Mareel_VPN_Duration', 'United-States_54.200.124.241_duration')]

    await asyncio.gather(*[consume(service, server) for service, server in servers])


if __name__ == "__main__":
    asyncio.run(main())
