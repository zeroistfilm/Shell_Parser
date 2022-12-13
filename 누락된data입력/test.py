# go.txt파일 읽기
import asyncio
import traceback

import aiokafka

with open('vpn.txt', 'r') as f:
    lines = f.readlines()

dataList=[]
for line in lines:
    try:
        topic = line.split(" b'")[0].split('send ')[1]
        message = bytes(line.split(" b'")[1][:-2], 'utf-8')
        dataList.append((topic, message))
    except:
        continue


async def message_send():
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=['146.56.42.103:29092', '146.56.42.103:29093', '146.56.42.103:29094'])
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