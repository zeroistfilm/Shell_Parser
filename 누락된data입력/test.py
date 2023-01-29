# go.txt파일 읽기
import asyncio
import traceback

import aiokafka

with open('nohup.out', 'r') as f:
    lines = f.readlines()

dataList=[]
for line in lines:
    try:
        topic = line.split(" b'")[0].split('send ')[1]
        message = bytes(line.split(" b'")[1][:-2], 'utf-8')
        # print(message)
        if (str(message).find('2023-01-26'))>0:
            dataList.append((topic, message))
    except:
        continue
print(dataList)
#send South-Korea_146.56.145.179_raw b'{"local_ip": "10.0.0.3", "server_ip": "146.56.145.179", "country": "South Korea", "detected_application_name": "Unknown", "detected_protocol_name": "HTTP/S", "host_server_name": "NULL", "dns_host_name": "NULL", "local_port": 62777, "other_ip": "104.76.96.30", "other_port": 443, "first_seen_at": "2023-01-26 18:45:36.816", "first_update_at": "2023-01-26 18:45:36.816", "last_seen_at": "2023-01-26 18:45:36.816", "game": "NULL", "game_company": "NULL", "digest": "1a64c2779dd90a7eea6555b561d230492e1c4fb7", "local_bytes": 0, "local_packets": 0, "other_bytes": 76, "other_packets": 1, "total_bytes": 76, "total_packets": 1}'

async def message_send():
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=['146.56.42.103:29092', '146.56.42.103:29093', '146.56.42.103:29094'], acks=1)
    # producer = aiokafka.AIOKafkaProducer(bootstrap_servers='3.34.72.6:29092', acks=0)
    # print(f"{title} producer start")
    try:
        # Japan_141.147.190.169_{raw or duration}
        # topic = "_".join(['-'.join(serverInfo['country'].split(' ')), serverInfo['ip'], title])
        await producer.start()
        for data in dataList:
            await producer.send(data[0], data[1])
            print(f'send', data[0], data[1])

            # data = await queue.get()
            # await producer.send(topic, data)
            # print(f'send', topic, data)
    except Exception as e:
        # trace_back = traceback.format_exc()
        # message = str(e) + "\n" + str(trace_back)
        print("kafka producer Error : ", message)

    finally:
        await producer.stop()
        # print(f"{title} producer end")


async def main():

    try:
        await asyncio.gather(*[ message_send()])
    except Exception as e:
        trace_back = traceback.format_exc()
        message = str(e) + "\n" + str(trace_back)
        print("asyncio Error : ", message)
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())