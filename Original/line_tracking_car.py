import asyncio
import json
import random
import time
from datetime import datetime

from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1 


BROKER_URL = "mqtt://10.12.5.225:1883"  
TOPIC_PUBLISH = "car/status"
TOPIC_SUBSCRIBE = "car/command"
SEND_INTERVAL = 3

class line_tracking_car:

    def __init__(self, broker_url, pub_topic, sub_topic):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.pub_topic = pub_topic
        self.sub_topic = sub_topic
        self.state = 'INIT'
        print(f"Initial state: {self.state}")

    async def listener_task(self):
        print("Listener task started. Car waiting for messages...")    
        while True:
            try:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                command = packet.payload.data.decode().strip().upper()
                print(f"✅ Received command: '{command}'")
                if command == 'START' and self.state == 'INIT':
                    print("MOVING")
                    self.state = 'MOVING'                   
                elif command == 'STOP' and self.state == 'MOVING':
                    print("STOPPED")  
                    self.state = 'STOPPED'               
                elif command == 'START' and self.state == 'STOPPED':
                    print("MOVING")
                    self.state = 'MOVING'   
                else:
                    print(f"--> Command '{command}' ignored (current state is '{self.state}')")
            except Exception as e:
                print(f"Listener error: {e}")
                break

    async def publisher_task(self):
        while True:
            try:
                current_state_str = self.state
                message_bytes = current_state_str.encode('utf-8')                
                await self.client.publish(self.pub_topic, message_bytes, qos=1)
                print(f"🚀 Published status to '{self.pub_topic}': '{current_state_str}'")                
                await asyncio.sleep(SEND_INTERVAL)
            except Exception as e:
                print(f"Publisher error: {e}")
                break

    async def run(self):        
        try:
            await self.client.connect(self.broker_url)
            await self.client.subscribe([(self.sub_topic, 1)])
            print(f"Connected to broker and subscribed to '{self.sub_topic}'")

            listener = asyncio.create_task(self.listener_task())
            publisher = asyncio.create_task(self.publisher_task())
            
            await asyncio.gather(listener, publisher)

        except Exception as e:
            print(f"An error occurred in main run: {e}")
        finally:
            print("Disconnecting client...")
            await self.client.disconnect() 

if __name__ == "__main__":
    car = line_tracking_car(BROKER_URL, TOPIC_PUBLISH, TOPIC_SUBSCRIBE)
    try:
        asyncio.run(car.run())
    except KeyboardInterrupt:
        print("\nProgram stopped by user (Ctrl+C).")