import asyncio
import json
import random
import time
from datetime import datetime
from pynput import keyboard
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1

BROKER_URL = "mqtt://localhost:1883"
TOPIC_PUBLISH = "car/command"
TOPIC_SUBSCRIBE = "car/status"

class controller:
    def __init__(self, broker_url, pub_topic, sub_topic):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.pub_topic = pub_topic
        self.sub_topic = sub_topic
        self.robot_ready_event = asyncio.Event()
        self.current_command = "STOP,0"
        self.key_pressed = set()

    def on_press(self, key):
        try:
            key_char = key.char
        except AttributeError:
            return

        self.key_pressed.add(key_char)
        
        if key_char == 'w':
            self.current_command = "FORWARD,0"
        elif key_char == 's':
            self.current_command = "BACKWARD,0"
        elif key_char == 'a':
            # self.current_command = "TURN_LEFT,50"
            self.current_command = "TURN_LEFT,70"
        elif key_char == 'd':
            # self.current_command = "TURN_RIGHT,50"
            self.current_command = "TURN_RIGHT,70"
        elif key_char == '1':
            self.current_command = "SPEED_RATE,1"
        elif key_char == '9':
            self.current_command = "SPEED_RATE,0.9"
        elif key_char == '8':
            self.current_command = "SPEED_RATE,0.8"
        elif key_char == '7':
            self.current_command = "SPEED_RATE,0.7"
        elif key_char == '6':
            self.current_command = "SPEED_RATE,0.6"
        elif key_char == 'm':
            self.current_command = "MANUAL,0"
        elif key_char == 'n':
            self.current_command = "AUTO,0"

    def on_release(self, key):
        try:
            self.key_pressed.remove(key.char)
        except (KeyError, AttributeError):
            pass
        
        # if not self.key_pressed:
        #     self.current_command = "STOP,0"

    async def listener_task(self):        
        print("Listener task started: Waiting for robot status updates...")
        while True:
            try:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                status = packet.payload.data.decode()                
                print(f"\n[STATUS UPDATE] Robot state: {status}\n> ", end="", flush=True)

                if status == 'INIT' and not self.robot_ready_event.is_set():
                    print("🤖 Robot is ready! Signaling publisher to start.")
                    self.robot_ready_event.set()
            except asyncio.CancelledError:
                print("Listener task cancelled.")
                break
            except Exception as e:
                print(f"Listener error: {e}")
                break

    async def keyboard_publisher_task(self):
        print("Keyboard controller started: Use W,A,S,D to control the car")
        while True:
            try:
                print(f"--> Sending command: '{self.current_command}'")
                await self.client.publish(
                    self.pub_topic, 
                    self.current_command.encode('utf-8'), 
                    qos=1
                )
                await asyncio.sleep(0.1)  # Small delay to prevent flooding
            except Exception as e:
                print(f"Publisher error: {e}")
                break

    async def run(self):        
        try:
            await self.client.connect(self.broker_url)
            await self.client.subscribe([(self.sub_topic, 1)])
            print(f"Controller connected and listening to '{self.sub_topic}'")

            # Start keyboard listener
            keyboard_listener = keyboard.Listener(
                on_press=self.on_press,
                on_release=self.on_release
            )
            keyboard_listener.start()

            listener = asyncio.create_task(self.listener_task())
            sender = asyncio.create_task(self.keyboard_publisher_task())            
            await sender            
            listener.cancel()
            keyboard_listener.stop()

        except Exception as e:
            print(f"An error occurred in main run: {e}")
        finally:
            print("Disconnecting controller client...")
            await self.client.disconnect()

if __name__ == "__main__":
    controller = controller(BROKER_URL, TOPIC_PUBLISH, TOPIC_SUBSCRIBE)
    try:
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        print("\nProgram stopped.")