import asyncio
import json
import random
import time
from datetime import datetime
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1 

BROKER_URL = "mqtt://10.12.5.225:1883"  
TOPIC_PUBLISH_B = "carB/status"
TOPIC_SUBSCRIBE_B = "carB/command"

class line_tracking_car:

    def __init__(self, broker_url, pub_topic, sub_topic):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.pub_topic = pub_topic
        self.sub_topic = sub_topic
        self.state = None
        self.value = None
        self.current_index = None
        self.path = [11,12,13,14,15] 
        if self.path:
            self.current_index = 0
            self.value = self.path[self.current_index] 
        else:
            self.current_index = -1
            self.value = None
    
    def update_checkpoint(self):
        if self.is_at_end():
            print("Action blocked: Already at the end of the path.")
            return False
        print(f"Action 'move': From index {self.path[self.current_index]} to {self.path[self.current_index + 1]}")
        self.current_index += 1
        self.value = self.path[self.current_index]
        return True

    def is_at_end(self):
        if self.current_index == -1:
            return True
        return self.current_index >= len(self.path) - 1
   
    async def listener_task(self):
        print("Listener task started. Car waiting for messages...")    
        self.state = f"checkpointB,{self.value}"
        await self.publisher_task()  
        time.sleep(10)
        while True:
            try:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                command = packet.payload.data.decode().strip()
                print(f"Received command: '{command}'")            
                #move
                if command == 'move':                    
                    print("moving")
                    time.sleep(5)
                    self.update_checkpoint()
                    self.state = f"checkpointB,{self.value}"
                    await self.publisher_task()               

                #turn and stop
                if command == 'turn right':
                    print("turn right done")
                    time.sleep(5)
                    self.state = "done,0"  
                    await self.publisher_task()
                elif command == 'turn left':
                    print("turn left done")
                    time.sleep(5)
                    self.state = "done,0"  
                    await self.publisher_task()
                elif command == 'stop' :
                    print("stopped")
                    time.sleep(5)
                    self.state = "stopped,0"  
                    await self.publisher_task()
            
            except Exception as e:
                print(f"Listener error: {e}")
                break

    async def publisher_task(self):
        try:
            current_state_str = self.state
            message_bytes = current_state_str.encode('utf-8')  
            print("log", message_bytes)              
            await self.client.publish(self.pub_topic, message_bytes, qos=1)
        #     while current_state_str != None:
        #         time.sleep(5)
        #     try:
        #         await self.client.publish(self.pub_topic, message_bytes, qos=1)
        #         print(f"Published status to '{self.pub_topic}': '{current_state_str}'")   
        #     except Exception as e:
        #         print(f"Error sending")                         
        except Exception as e:
            print(f"Publisher error: {e}")

    async def run(self):        
        try:
            await self.client.connect(self.broker_url)
            await self.client.subscribe([(self.sub_topic, 1)])
            print(f"Connected to broker and subscribed to '{self.sub_topic}'")

            listener = asyncio.create_task(self.listener_task())
            # publisher = asyncio.create_task(self.publisher_task())
            
            # await asyncio.gather(listener, publisher)
            await asyncio.gather(listener)

        except Exception as e:
            print(f"An error occurred in main run: {e}")
        finally:
            print("Disconnecting client...")
            await self.client.disconnect() 

if __name__ == "__main__":
    car = line_tracking_car(BROKER_URL, TOPIC_PUBLISH_B, TOPIC_SUBSCRIBE_B)
    try:
        asyncio.run(car.run())
    except KeyboardInterrupt:
        print("\nProgram stopped by user (Ctrl+C).")