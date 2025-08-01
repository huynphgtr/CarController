import asyncio
import json
import random
import time
from datetime import datetime
import requests
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1

MAP_API_URL = "https://hackathon.omelet.tech/api/maps/"
BROKER_URL = "mqtt://10.12.5.225:1883"  
TOPIC_PUBLISH_A = "carA/command"
TOPIC_SUBSCRIBE_A = "carA/status"
TOPIC_PUBLISH_B = "carB/command"
TOPIC_SUBSCRIBE_B = "carB/status"
SEND_INTERVAL = 10
MOVE_DURATION = 20
STOP_DURATION = 10

class controller:
    def __init__(self, broker_url, pub_topic_A, sub_topic_A, pub_topic_B, sub_topic_B ):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.pub_topic_A = pub_topic_A
        self.sub_topic_A = sub_topic_A
        self.pub_topic_B = pub_topic_B
        self.sub_topic_B = sub_topic_B
        self.robot_ready_event = asyncio.Event()

        # --- Cấu trúc dữ liệu cho bản đồ ---
        self.map_data = None      # Lưu trữ JSON thô từ API
        self.graph = {}           # Danh sách kề để tìm đường: {node_id: {neighbor_id: 'direction', ...}}
        self.nodes_info = {}      # Lưu trữ chi tiết các node: {node_id: {'x': x, 'y': y, 'type': type}, ...}

        self.robot_status = None      
        self.robot_value = None        
        self.current_position = None   

        self.direction = 1 #change direction N:0 E:1 S:2 W:3
        self.path = None
        self.edge = None
        self.current_position = None
        self.current_edge = None
    
    async def fetch_map(self, url):

        print(f"Fetching map from {url}...")
        try:
            # Chạy hàm requests.get (đồng bộ) trong một thread riêng để không chặn asyncio
            response = await asyncio.to_thread(requests.get, url)
            response.raise_for_status()  # Ném ra lỗi nếu status code là 4xx hoặc 5xx

            json_data = response.json()
            if "results" in json_data and len(json_data["results"]) > 0:
                print("Map data fetched successfully.")
                return json_data["results"][0]
            else:
                print("Error: No map data found in API response 'results'.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching map from API: {e}")
            return None
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON from API response.")
            return None

    def extract_map(self, map_data):
        """
        Xử lý dữ liệu bản đồ đã có:
        1. Điền vào cấu trúc dữ liệu self.graph và self.nodes_info.
        2. Tạo file HTML trực quan hóa.
        Trả về True nếu thành công, False nếu thất bại.
        """
        print("Processing and extracting map data...")
        if not map_data:
            print("Error: Cannot extract map from empty data.")
            return False

        try:
            # Lưu dữ liệu map thô
            self.map_data = map_data

            # 1. Trích xuất thông tin node và xây dựng cấu trúc graph
            for node in self.map_data.get('nodes', []):
                self.nodes_info[node['id']] = {'x': node['x'], 'y': node['y'], 'type': node['type']}
                self.graph[node['id']] = {}

            # 2. Xây dựng danh sách kề từ các cạnh (edges)
            for edge in self.map_data.get('edges', []):
                source_id = edge['source']
                target_id = edge['target']
                label = edge['label']  # Hướng đi ('N', 'E', 'S', 'W')
                if source_id in self.graph:
                    self.graph[source_id][target_id] = label
                else:
                    print(f"Warning: Source node {source_id} from an edge was not found in the nodes list.")

            print(f"Map extracted successfully: {len(self.nodes_info)} nodes, {len(self.map_data.get('edges', []))} edges.")


            return True
        except Exception as e:
            print(f"An error occurred during map extraction: {e}")
            return False
    
    async def listener_task(self):
        print("Listener task started: Waiting for robot status updates...")
        while True:
            try:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                payload_str = packet.payload.data.decode()    
                # payload_data = json.loads(payload_str)
                # print(f"\n[RAW PAYLOAD RECEIVED] '{payload_data}'")   
                print(f"\n[RAW STATUS RECEIVED] '{payload_str}'")                        
                status_type = None
                value = None
                parts = payload_str.split(',')
                                  
                status_type = parts[0].strip()
                try:
                    value = int(parts[1].strip())
                except ValueError:
                    value = parts[1].strip()
                    print(f"[PARSING WARNING] Value '{value}' is not an integer.")
                
                self.robot_status = status_type
                self.robot_value = value
                
                print(f"  -> Parsed: Status='{self.robot_status}', Value={self.robot_value}")
                
                # is_valid_checkpoint = self.check_checkpoint()
                # if not is_valid_checkpoint:
                #     print("!! WARNING: Invalid or out-of-sequence checkpoint received. Robot may be off-track. !!")
                if self.robot_status == 'checkpointA' and self.check_checkpoint():
                    print(f"Robot has reached valid checkpoint {self.robot_value}. Updating position.")
                    self.current_position = self.robot_value
                    self.reset_direction()            
                    if (self.next_edge() - self.direction) == 0:
                        print("self direction", self.direction)
                        await self.send_move_command()   
                        self.direction = self.next_edge()                     
                    elif ((self.next_edge() - self.direction) == 1) or ((self.next_edge() - self.direction) == -3) :   
                        print("self direction", self.direction)                                            
                        await self.send_turnright_command()   
                        self.direction = self.next_edge()                     
                    elif ((self.next_edge() - self.direction) == -1) or ((self.next_edge() - self.direction) == 3):                        
                        await self.send_turnleft_command()   
                        self.direction = self.next_edge()                     
                    elif self.next_edge() == -99:                         
                        await self.send_stop_command()
                        self.direction = self.next_edge()    
                if self.robot_status == "done": 
                    await self.send_move_command()              
                elif self.robot_status == "stopped": 
                    print("Car go to destination")
            except asyncio.CancelledError:
                print("Listener task cancelled.")
                break
            except Exception as e:
                print(f"Listener error: {e}")
                break

    def reset_direction(self): 
        if self.current_position == self.path[0]: 
            print("change direction from", self.direction, "to 1")
            self.direction = 1
            
    def check_checkpoint(self):        
        if self.robot_value in self.path:            
            return True
        else:
            print(f"[VALIDATION FAILED] Checkpoint {self.robot_value} is NOT in the designated path {self.path}.")
            return False

    def next_edge(self):
        try:
            index = self.path.index(self.current_position)
            if index == len(self.path) - 1:
                next_edge_value = -99
            else:
                next_edge_value = self.edge[index]
            print("position:", self.current_position)
            return next_edge_value
        except ValueError:
            print(f"Error: Current position {self.current_position} not found in path.")
            return None 
    
    async def send_move_command(self):
        command = 'move'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(self.pub_topic_A, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")

    async def send_turnright_command(self):
        command = 'turn right'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(self.pub_topic_A, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")

    async def send_turnleft_command(self):
        command = 'turn left'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(self.pub_topic_A, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")
  
    async def send_stop_command(self):
        command = 'stop'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(self.pub_topic_A, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")
    
    async def automatic_publisher_task(self):
        print("Publisher task started: Waiting for robot ready signal...")        
        await self.robot_ready_event.wait()        
        print("Signal received! Starting automated command cycle.")        
        await self.send_move_command() 

        while self.current_position != self.path[-1]:
            await asyncio.sleep(1)     
        print(f"Publisher confirms destination {self.path[-1]} reached! Sending final 'STOP' command.")
        try:
            await self.client.publish(self.pub_topic_A, b'STOP', qos=QOS_1)
        except Exception as e:
            print(f"Error sending 'STOP' command: {e}")
        
        print("Publisher task finished.")

    async def run(self):
        # map_data = await self.fetch_map(MAP_API_URL)
        # if not map_data or not self.extract_map(map_data):
        #     print("Failed to initialize map. Exiting.")
        #     return       
        self.path = [1,2,3,8,9,4,5] 
        self.edge = [1,1,2,1,0,1]        
        
        try:
            await self.client.connect(self.broker_url)
            await self.client.subscribe([(self.sub_topic_A, 1)])
            print(f"Controller connected and listening to '{self.sub_topic_A}'")

            listener = asyncio.create_task(self.listener_task())
            sender = asyncio.create_task(self.automatic_publisher_task())            
            await sender            
            listener.cancel()

        except Exception as e:
            print(f"An error occurred in main run: {e}")
        finally:
            print("Disconnecting controller client...")
            await self.client.disconnect()

if __name__ == "__main__":
    controller = controller(BROKER_URL, TOPIC_PUBLISH_A, TOPIC_SUBSCRIBE_A, TOPIC_PUBLISH_B, TOPIC_SUBSCRIBE_B)
    try:
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        print("\nProgram stopped.")