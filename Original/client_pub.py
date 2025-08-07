import asyncio
import time
from datetime import datetime
from amqtt.client import MQTTClient

TOPIC = "latency/test"
BROKER_URL = "mqtt://192.168.50.59:1883" 
MESSAGE_COUNT = 30000 

async def main():
    client = MQTTClient()
    try:
        print(f"Connecting to MQTT broker at {BROKER_URL}...")
        await client.connect(BROKER_URL)
        print("Connected successfully!")
        
        print(f"\n--- Preparing to send test sequence ---")

        start_timestamp = time.time()
        await client.publish(TOPIC, b'start', qos=1)
        start_realtime = datetime.fromtimestamp(start_timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"Sent 'start' signal at: {start_realtime}")
        for i in range(MESSAGE_COUNT - 2):
            message_bytes = f"data_{i+1}".encode('utf-8')
            await client.publish(TOPIC, message_bytes, qos=1)
        print(f"Sent {MESSAGE_COUNT - 2} data messages.")

        await client.publish(TOPIC, b'end', qos=1)
        end_timestamp = time.time() 
        end_realtime = datetime.fromtimestamp(end_timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"Sent 'end' signal at:   {end_realtime}")

        total_duration = end_timestamp - start_timestamp
        print("\n--- Publisher Summary ---")
        print(f"Total send time (from 'start' to 'end'): {total_duration:.4f} seconds.\n")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Disconnecting from broker...")
        await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram stopped by user.")