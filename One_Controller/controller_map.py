import asyncio
import json
import random
import time
from datetime import datetime
import requests
from typing import Dict, List, Any, Optional
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1
from collision_avoidance import MultiAGVPlanner

MAP_API_URL = "https://hackathon.omelet.tech/api/maps/"
BROKER_URL = "mqtt://10.12.5.225:1883"  
TOPIC_PUBLISH_A = "carA/command"
TOPIC_SUBSCRIBE_A = "carA/status"
TOPIC_PUBLISH_B = "carB/command"
TOPIC_SUBSCRIBE_B = "carB/status"
TARGET_URL = "https://hackathon.omelet.tech/api/maps/980778ba-4ce1-4094-81d8-aa1f6da40b93/"
SPEEDS = {"AGV1": 1.0, "AGV2": 1.0}
DIRECTION_MAP = {"N": 0, "E": 1, "S": 2, "W": 3}

class Controller:
    def __init__(self, broker_url, pub_topic_A, sub_topic_A, pub_topic_B, sub_topic_B, path_A, path_B, edge_A, edge_B):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.sub_topicA = sub_topic_A
        self.sub_topicB = sub_topic_B
        self.controllerA = CarController( pub_topic=pub_topic_A, path=path_A, edge=edge_A,  client=self.client )
        self.controllerB = CarController( pub_topic=pub_topic_B, path=path_B, edge=edge_B, client=self.client )
        self.robot_ready_eventA = asyncio.Event()
        self.robot_ready_eventB = asyncio.Event()

        # self.map_data = None      # Lưu trữ JSON thô từ API
        # self.graph = {}           # Danh sách kề để tìm đường: {node_id: {neighbor_id: 'direction', ...}}
        # self.nodes_info = {}      # Lưu trữ chi tiết các node: {node_id: {'x': x, 'y': y, 'type': type}, ...}

        self.robot_status = None      
        self.robot_value = None        
        self.current_position = None   

        # self.direction = 1 #change direction N:0 E:1 S:2 W:3
        # self.current_position = None
        # self.current_edge = None
    
    def get_agv_paths(map_url: str, agv_speeds: dict = None) -> dict | None:
        """
        Uses the MultiAGVPlanner class to generate a plan and extract the paths.
        Returns:
            A dictionary mapping each AGV ID to its path list, or None on failure.
        """        
        planner = MultiAGVPlanner(map_url, agv_speeds or {})
        print("Fetching and building map data...")
        if not planner.fetch_map_data():
            print("Failed to fetch map data.")
            return None        
        try:
            planner.build_adjacency_list()
        except ValueError as e:
            print(f"Error building graph: {e}")
            return None
        print("Generating the AGV plan...")
        try:
            full_plan = planner.generate_multi_agv_plan()
        except ValueError as e:
            print(f"Error generating plan: {e}")
            return None

        agv_paths = {}
        if full_plan and "agv_plans" in full_plan:
            for agv_plan in full_plan["agv_plans"]:
                agv_id = agv_plan["agv_id"]
                path = agv_plan["movements"][0]["path"]
                agv_paths[agv_id] = path

        return agv_paths
    
    def get_directions_from_paths(map_url: str, agv_paths: Dict[Any, List[int]]) -> Optional[Dict[Any, List[int]]]:
        """
        Fetches a map from a URL and converts AGV paths (lists of nodes) into 
        sequences of numerical directions.

        Args:
            map_url: The URL to fetch the JSON map data from.
            agv_paths: A dictionary mapping an AGV identifier to its path,
                    where a path is a list of node integers.
                    Example: {'AGV1': [1, 2, 3], 'AGV2': [8, 7, 6]}

        Returns:
            A dictionary mapping each AGV identifier to a list of numerical directions
            corresponding to its path, or None if an error occurs.
            Example: {'AGV1': [1, 1], 'AGV2': [3, 3]}
        """
        # 1. Fetch and parse the map data from the URL
        try:
            response = requests.get(map_url)
            response.raise_for_status()
            map_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching map data: {e}")
            return None

        # 2. Get the "edges" and build an efficient lookup table
        # The lookup table maps a (source, target) tuple to its label (e.g., 'E')
        # This is much faster than searching the list of edges for every step in a path.
        if "edges" not in map_data:
            print("Error: 'edges' key not found in map data.")
            return None        
        edge_label_lookup = {}
        try:
            for edge in map_data["edges"]:
                edge_label_lookup[(edge["source"], edge["target"])] = edge["label"]
        except (KeyError, TypeError) as e:
            print(f"Error: Malformed edge data in map. Details: {e}")
            return None

        # 3. Process each AGV path to convert it to directions
        all_agv_directions = {}
        for agv_id, path in agv_paths.items():
            if not path or len(path) < 2:
                all_agv_directions[agv_id] = [] # Path is too short to have movements
                continue
                
            directions_list = []
            # Iterate through the path to get pairs of (source, target) nodes
            for i in range(len(path) - 1):
                source_node = path[i]
                target_node = path[i+1]
                
                # Find the label for the current movement (e.g., 1 -> 2)
                label = edge_label_lookup.get((source_node, target_node))
                
                if label is None:
                    print(f"Warning: No edge found for movement {source_node}->{target_node} in AGV '{agv_id}' path. Skipping this step.")
                    continue

                # Convert the label ('N', 'E', 'S', 'W') to its corresponding number
                direction_code = DIRECTION_MAP.get(label)
                
                if direction_code is None:
                    print(f"Warning: Unknown label '{label}' for movement {source_node}->{target_node}. Skipping this step.")
                    continue
                
                directions_list.append(direction_code)
                
            all_agv_directions[agv_id] = directions_list

        return all_agv_directions    
    
    async def listener_task(self):
        print("Listener task started: Waiting for robot status updates...")
        while True:
            try:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                topic_name = packet.variable_header.topic_name #get topic name
                payload_str = packet.payload.data.decode()       
                
                print(f"\n[LISTEN FROM] '{topic_name}'")
                if(topic_name == "carA/status"): 
                    
                    await self.controllerA.proccess(payload_str)
                elif(topic_name == "carB/status"): 
                    await self.controllerB.proccess(payload_str)               
                
            except asyncio.CancelledError:
                print("Listener task cancelled.")
                break
            except Exception as e:
                print(f"Listener error: {e}")
                break
        
    async def automatic_publisher_task(self):
        print("Publisher task started: Waiting for robot ready signal...")        
        await self.robot_ready_eventA.wait()
        await self.robot_ready_eventB.wait()         
        print("Signal received! Starting automated command cycle.")        
        # await self.send_move_command() 

        # while self.current_position != self.path[-1]:
        #     await asyncio.sleep(1)     
        # print(f"Publisher confirms destination {self.path[-1]} reached! Sending final 'STOP' command.")
        try:
            await asyncio.gather(
            self.client.publish(self.pub_topic_A, b'STOP', qos=QOS_1),
            self.client.publish(self.pub_topic_B, b'STOP', qos=QOS_1)
            )
            print(f"Sent 'STOP' command to '{self.pub_topic_A}' and '{self.pub_topic_B}'")
        except Exception as e:
            print(f"Error sending 'STOP' commands: {e}")
        print("Publisher task finished.")

    async def run(self):
        # map_data = await self.fetch_map(MAP_API_URL)
        # if not map_data or not self.extract_map(map_data):
        #     print("Failed to initialize map. Exiting.")
        #     return       
        # self.path = [1,2,3,8,9,4,5] 
        # self.edge = [1,1,2,1,0,1]        
        
        try:
            await self.client.connect(self.broker_url)
            await self.client.subscribe([(self.sub_topicA, 1)])
            await self.client.subscribe([(self.sub_topicB, 1)])
            print(f"Controller connected and listening to '{self.sub_topicA}'")
            print(f"Controller connected and listening to '{self.sub_topicB}'")

            listener = asyncio.create_task(self.listener_task())
            sender = asyncio.create_task(self.automatic_publisher_task())                    
            await sender         
            listener.cancel()

        except Exception as e:
            print(f"An error occurred in main run: {e}")
        finally:
            print("Disconnecting controller client...")
            await self.client.disconnect()

class CarController: 
    def __init__(self, pub_topic, path, edge, client ):
        self.client = client
        self.pub_topic = pub_topic

        self.robot_status = None      
        self.robot_value = None        
        self.current_position = None   

        self.path = path
        self.edge = edge
   
    def check_checkpoint(self, path, value):        
        if value in path:            
            return True
        else:
            print(f"[VALIDATION FAILED] Checkpoint {value} is NOT in the designated path {path}.")
            return False
    
    def next_edge(self, path, current_position,edge):
        try:
            index = path.index(current_position)
            if index == len(path) - 1:
                next_edge_value = -99
            else:
                next_edge_value = edge[index]
            return next_edge_value
        except ValueError:
            print(f"Error: Current position {current_position} not found in path.")
            return None 
    
    def reset_direction(self, path,current_position,direction): 
        if current_position == path[0]: 
            print("change direction from", direction, "to 1")
            direction = 1

    async def send_move_command(self,pub_topic):
        command = 'move'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(pub_topic, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")

    async def send_turnright_command(self,pub_topic):
        command = 'turn right'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(pub_topic, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")

    async def send_turnleft_command(self,pub_topic):
        command = 'turn left'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(pub_topic, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")
  
    async def send_stop_command(self,pub_topic):
        command = 'stop'
        try:
            print(f"--> Sending command: '{command}'")
            await self.client.publish(pub_topic, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")
    
    async def proccess(self, payload_str): 
        print(f"\n[RAW STATUS RECEIVED] '{payload_str}'")    
        direction = 1 
        # path = [1,2,3,8,9,4,5]
        # edge = [1,1,2,1,0,1]
        current_position = None
        # current_edge = None                    
        status_type = None
        value = None
        parts = payload_str.split(',')                            
        status_type = parts[0].strip()
        pub_topic = self.pub_topic
        try:
            value = int(parts[1].strip())
        except ValueError:
            value = parts[1].strip()
            print(f"[PARSING WARNING] Value '{value}' is not an integer.")
        
        # self.robot_status = status_type
        # self.robot_value = value
        
        print(f"  -> Parsed: Status='{status_type}', Value={value}")
        
        # is_valid_checkpoint = self.check_checkpoint()
        # if not is_valid_checkpoint:
        #     print("!! WARNING: Invalid or out-of-sequence checkpoint received. Robot may be off-track. !!")
        if status_type == 'checkpoint' and self.check_checkpoint(self.path,value):
            print(f"Robot has reached valid checkpoint {value}. Updating position.")
            current_position = value
            self.reset_direction(self.path,current_position,direction)            
            if (self.next_edge(self.path, current_position,self.edge) - direction) == 0:
                await self.send_move_command(pub_topic)   
                self.direction = self.next_edge(self.path, current_position,self.edge)                     
            elif ((self.next_edge(self.path, current_position,self.edge) - direction) == 1) or ((self.next_edge(self.path, current_position,self.edge) - direction) == -3) :                                              
                await self.send_turnright_command(self.pub_topic)   
                self.direction = self.next_edge(self.path, current_position,self.edge)                     
            elif ((self.next_edge(self.path, current_position,self.edge) - direction) == -1) or ((self.next_edge(self.path, current_position,self.edge) - direction) == 3):                        
                await self.send_turnleft_command(pub_topic)   
                self.direction = self.next_edge(self.path, current_position,self.edge)                     
            elif self.next_edge(self.path, current_position,self.edge) == -99:                         
                await self.send_stop_command(pub_topic)
                self.direction = self.next_edge(self.path, current_position,self.edge)    
        if status_type == "done": 
            await self.send_move_command(pub_topic)              
        elif status_type == "stopped": 
            print("Car go to destination")
    
if __name__ == "__main__":    
    #get paths
    extracted_paths = Controller.get_agv_paths(TARGET_URL, SPEEDS)
    PATH_A = extracted_paths['AGV1']
    PATH_B = extracted_paths['AGV2']
    print("Path A: ", PATH_A)    
    print("Path B: ", PATH_B)    

    #get directions
    directions = Controller.get_directions_from_paths(TARGET_URL, extracted_paths)
    EDGE_A = directions['AGV1']
    EDGE_B = directions['AGV2']
    print("Edge A: ", EDGE_A)    
    print("Edge B: ", EDGE_B)    

    controller = Controller(BROKER_URL, TOPIC_PUBLISH_A, TOPIC_SUBSCRIBE_A, TOPIC_PUBLISH_B, TOPIC_SUBSCRIBE_B, PATH_A, PATH_B, EDGE_A, EDGE_B)
    try:
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        print("\nProgram stopped.")