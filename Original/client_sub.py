import asyncio
from amqtt.client import MQTTClient

async def main():
    client = MQTTClient()
    await client.connect("mqtt://10.12.5.225:1883")
    await client.subscribe([("car/command", 1)])
    await client.public([("car/status"), 1])

    print("Subscribed to car/command. Waiting for messages...")
    while True:
        message = await client.deliver_message()
        
        packet = message.publish_packet
        print(f"Received: {packet.payload.data.decode()}")

asyncio.run(main())