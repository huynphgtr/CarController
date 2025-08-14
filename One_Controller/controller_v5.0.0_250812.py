import asyncio
import heapq
from itertools import chain
import json
import random
import time
from datetime import datetime
import traceback
import requests
from typing import Dict, List, Any, Optional, Tuple
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_1
from collision_avoidance import MultiAGVPlanner

BROKER_URL = "mqtt://10.12.5.225:1883"  
TOPIC_PUBLISH_A = "carA/command"
TOPIC_SUBSCRIBE_A = "carA/status"
TOPIC_PUBLISH_B = "carB/command"
TOPIC_SUBSCRIBE_B = "carB/status"
TARGET_URL = "http://127.0.0.1:5500/One_Controller/map_v2.0.0_250812.json"
SPEEDS = {"AGV1": 1.0, "AGV2": 1.0}
DIRECTION_MAP = {"N": 0, "E": 1, "S": 2, "W": 3} 
 
class MyMultiAGVPlanner(MultiAGVPlanner):
    def __init__(self, map_url: str, agv_speeds: Optional[Dict[str, float]] = None):
        super().__init__(map_url, agv_speeds)  
        self.pre_visited = set()
        
        # car plan
        self.planner = None
        # self.assignment = assignment
        self.assignment = None
        # path = MyMultiAGVPlanner.extract_path(planner, assignment) 
        # direction = MyMultiAGVPlanner.extract_direction(map_url=TARGET_URL,agv_paths=path)
        # self.path = path['AGV1']
        # self.direction = direction['AGV1']
        # self.start = list(assignment.keys())[0]
        # self.destination = list(assignment.values())[0]
        self.start = None
        self.destination = None
        self.current_position = self.start
        # self.update_path()
        self.path = None
        self.direction = None

    def dijkstra(self, start: str, end: str) -> Tuple[List[str], float]:
        """
        Find shortest path using Dijkstra's algorithm with caching
        """
        # Check cache
        cache_key = (start, end)
        if cache_key in self.distance_cache:
            return self.distance_cache[cache_key]

        if start not in self.adjacency_list or end not in self.adjacency_list:
            return [], float("inf")

        # Distance from start to each node
        distances = {node: float("inf") for node in self.adjacency_list}
        distances[start] = 0

        # Previous node in optimal path
        previous = {node: None for node in self.adjacency_list}

        # Priority queue: (distance, node)
        pq = [(0, start)]
        visited = self.pre_visited # set()

        while pq:
            current_distance, current_node = heapq.heappop(pq)

            if current_node in visited:
                continue

            visited.add(current_node)

            # Found destination
            if current_node == end:
                break

            # Check neighbors
            for neighbor, weight in self.adjacency_list[current_node]:
                distance = current_distance + weight

                if distance < distances[neighbor]:
                    distances[neighbor] = distance
                    previous[neighbor] = current_node
                    heapq.heappush(pq, (distance, neighbor))

        # Reconstruct path
        path = []
        current = end
        while current is not None:
            path.append(current)
            current = previous[current]

        if path[-1] != start:  # No path found
            result = ([], float("inf"))
        else:
            path.reverse()
            result = (path, distances[end])

        # Cache result
        self.distance_cache[cache_key] = result
        return result

    def extract_path(self, planner, assignments: Dict[str, str]) -> dict | None:    
        try:
            full_plan = planner.generate_multi_agv_plan(assignments)
            agv_paths = {}
            if full_plan and "agv_plans" in full_plan:
                for agv_plan in full_plan["agv_plans"]:
                    agv_id = agv_plan["agv_id"]
                    path = agv_plan["movements"][0]["path"]
                    agv_paths[agv_id] = path
            planner.defined_visited = set(path)
        except ValueError as e:
            print(f"Error generating plan: {e}")
            return None

        return agv_paths  
    
    def extract_direction(self, map_url: str, agv_paths: Dict[Any, List[int]]) -> Optional[Dict[Any, List[int]]]:
        # 1. Fetch and parse the map data from the URL
        try:
            response = requests.get(map_url)
            response.raise_for_status()
            map_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching map data: {e}")
            return None

        # 2. Get the "edges" and build an efficient lookup table
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

class Controller:
    def __init__(self, broker_url, pub_topic_A, sub_topic_A, pub_topic_B, sub_topic_B):
        self.client = MQTTClient()
        self.broker_url = broker_url
        self.pub_topic_A = pub_topic_A
        self.sub_topicA = sub_topic_A
        self.pub_topic_B = pub_topic_B
        self.sub_topicB = sub_topic_B
        # self.controllerA = CarController( pub_topic=pub_topic_A, path=path_A, edge=edge_A,  client=self.client, car_plan= car1)
        # self.controllerB = CarController( pub_topic=pub_topic_B, path=path_B, edge=edge_B, client=self.client , car_plan=car2)
        self.car_planner1 = MyMultiAGVPlanner(map_url=TARGET_URL, agv_speeds=SPEEDS)
        self.car_planner2 = MyMultiAGVPlanner(map_url=TARGET_URL, agv_speeds=SPEEDS) 

        self.controllerA = CarController(self.pub_topic_A, self.client, self.car_planner1)
        self.controllerB = CarController(self.pub_topic_B, self.client, self.car_planner2) 
        self.robot_ready_eventA = asyncio.Event()
        self.robot_ready_eventB = asyncio.Event()

        # self.robot_status = None      
        # self.robot_value = None        
        # self.current_position = None  
        # self.car_planners = []
        # self.car_planners.append(MyMultiAGVPlannerV2(map_url=TARGET_URL, agv_speeds=SPEEDS))
        # self.car_planners.append(MyMultiAGVPlannerV2(map_url=TARGET_URL, agv_speeds=SPEEDS)) 
            
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
                    self.controllerB.another_checkpoints = self.controllerA.path  
                    await self.controllerB.proccess(payload_str)               
                
            except asyncio.CancelledError:
                print("Listener task cancelled.")
                break
            except Exception as e:
                print(f"Listener error: {e}")
                traceback.print_exc() 
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

        self.controllerA.fetch_map()
        self.controllerA.get_path_direction([], self.controllerA.car_plan.start_nodes[0], 
                                            self.controllerA.car_plan.destination_nodes[0])

        self.controllerB.fetch_map()
        self.controllerB.get_path_direction( #self.controllerA.path,
                                            [],
                                            self.controllerB.car_plan.start_nodes[1], 
                                            self.controllerB.car_plan.destination_nodes[1])
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
    def __init__(self, pub_topic, client, car_plan: MyMultiAGVPlanner):
        self.client = client
        self.pub_topic = pub_topic

        # self.robot_status = None      
        # self.robot_value = None        
        self.current_position = None   
        self.destination = None
        self.next_position = None
        self.path = None
        self.edge = None
        self.direction = None
        # self.visited_checkpoints = []
        self.another_checkpoints = []
        self.car_plan = car_plan

    def fetch_map(self):
        print("Fetching and building map data...")
        if not self.car_plan.fetch_map_data():
            print("Failed to fetch map data.") 
        try:
            self.car_plan.build_adjacency_list()
        except ValueError as e:
            print(f"Error building graph: {e}")
    
    def get_path_direction(self, pre_visited_path, start, end):
        assignment1 = {start:end}
        try:   
            self.car_plan.pre_visited = set(pre_visited_path)            
            full_plan = self.car_plan.generate_multi_agv_plan(assignment1)
            agv_paths = {}
            if full_plan and "agv_plans" in full_plan:
                for agv_plan in full_plan["agv_plans"]:
                    agv_id = agv_plan["agv_id"]
                    path = agv_plan["movements"][0]["path"]
                    agv_paths[agv_id] = path
        except ValueError as e:
            print(f"Error generating plan: {e}")        
        print(path)
        print(f"Found {len(self.car_plan.start_nodes)} start nodes: {self.car_plan.start_nodes}")
        print(f"Found {len(self.car_plan.destination_nodes)} start nodes: {self.car_plan.destination_nodes}")
        
        direction = self.car_plan.extract_direction(map_url=TARGET_URL,agv_paths=agv_paths)
        self.car_plan.direction = direction['AGV1']
        self.path = path
        self.edge = self.car_plan.direction
        
    def update_path(self):
        if self.path and len(self.path) > 1:   
            print("Another cp: ", self.another_checkpoints)         
            self.get_path_direction(self.another_checkpoints, 
                                self.current_position, 
                                self.destination)
        # Recalculate path from current_position to destination
        # self.next_position = self.path[1]
        # print("next position: ", self.next_position)

        # self.car_plan = MyMultiAGVPlannerV2(map_url=TARGET_URL, agv_speeds=SPEEDS)
        # if self.path and len(self.path) > 1:
        #     self.path = self.path[1:]
        #     print(self.path)
        #     self.edge = self.edge[1:]
        #     print(self.edge)
        # path, _ = self.car_plan.dijkstra(self.next_position, self.destination)
        # print("path after udating", path)
        # direction = self.car_plan.extract_direction(map_url=TARGET_URL, agv_paths={'AGV1': self.path})
        
        # self.path = path
        # self.direction = self.direction[1:]
    
    def check_checkpoint(self, path, value):        
        if value in path:            
            return True
        else:
            print(f"[VALIDATION FAILED] Checkpoint {value} is NOT in the designated path {path}.")
            return False
    
    def next_direction(self, path, current_position, edge):
        # print("Edge", edge)
        try:
            index = path.index(current_position)
            # print("index ", index, edge)
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
    
    async def send_command(self,pub_topic,command):
        try:
            print(f"--> Sending: '{command}'")
            await self.client.publish(pub_topic, command.encode('utf-8'), qos=QOS_1)
        except Exception as e:
            print(f"Error sending '{command}' command: {e}")
        
    async def proccess(self, payload_str): 
        print(f"\n[RAW STATUS RECEIVED] '{payload_str}'")    
        direction = 1 
        self.direction = direction
        current_position = None                 
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
            self.current_position = current_position 
            self.destination = self.path[-1] 
            # print(f"Current position: {self.current_position}, Destination: {self.destination}")
            # self.car_plan.current_position = current_position  
            # print("Check", self.car_plan.path)    
            # self.reset_direction(self.path,current_position,direction)            
            if (self.next_direction(self.path, current_position,self.edge) - direction) == 0:
                await self.send_command(pub_topic, "move")   
                self.direction = self.next_direction(self.path, current_position,self.edge)                     
            elif ((self.next_direction(self.path, current_position,self.edge) - direction) == 1) or ((self.next_direction(self.path, current_position,self.edge) - direction) == -3) :                                              
                await self.send_command(pub_topic, "turn right")   
                self.direction = self.next_direction(self.path, current_position,self.edge)                     
            elif ((self.next_direction(self.path, current_position,self.edge) - direction) == -1) or ((self.next_direction(self.path, current_position,self.edge) - direction) == 3):                           
                await self.send_command(pub_topic, "turn left")
                self.direction = self.next_direction(self.path, current_position,self.edge)                     
            elif self.next_direction(self.path, current_position,self.edge) == -99:                         
                await self.send_command(pub_topic, "stop")
                self.direction = self.next_direction(self.path, current_position,self.edge)   
            # if (self.next_edge(self.car_plan.path ,self.car_plan.current_position,self.car_plan.direction) - direction) == 0:
            #     await self.send_move_command(pub_topic)   
            #     self.direction = self.next_edge(self.car_plan.path , self.car_plan.current_position,self.car_plan.direction)                     
            # elif ((self.next_edge(self.car_plan.path , self.car_plan.current_position,self.car_plan.direction) - direction) == 1) or ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == -3) :                                              
            #     await self.send_turnright_command(self.pub_topic)   
            #     self.direction = self.next_edge(self.car_plan.path ,self.car_plan.current_position,self.car_plan.direction)                     
            # elif ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == -1) or ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == 3):                        
            #     await self.send_turnleft_command(pub_topic)   
            #     self.direction = self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction)                     
            # elif self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) == -99:                         
            #     await self.send_stop_command(pub_topic)
            #     self.direction = self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction)    
            # if (self.next_edge(self.path,self.current_position,self.direction) - direction) == 0:
            #     await self.send_move_command(pub_topic)   
            #     self.direction = self.next_edge(self.path,self.current_position,self.direction)                     
            # elif ((self.next_edge(self.car_plan.path,self.car_plan.current_position,self.car_plan.direction) - direction) == 1) or ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == -3) :                                              
            #     await self.send_turnright_command(self.pub_topic)   
            #     self.direction = self.next_edge(self.car_plan.path ,self.car_plan.current_position,self.car_plan.direction)                     
            # elif ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == -1) or ((self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) - direction) == 3):                        
            #     await self.send_turnleft_command(pub_topic)   
            #     self.direction = self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction)                     
            # elif self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction) == -99:                         
            #     await self.send_stop_command(pub_topic)
            #     self.direction = self.next_edge(self.car_plan.path, self.car_plan.current_position,self.car_plan.direction)    
        # self.visited_checkpoints.append(self.current_position)
        # print("Visited checkpoint: ", self.visited_checkpoints)
        self.update_path()

        # self.get_path_direction(another_checkpoints)
        # self.get_path_direction(self.visited_checkpoints, 0)
        # print("current path", self.path)   

        if status_type == "done": 
            await self.send_command(pub_topic,"move")              
        elif status_type == "stopped": 
            print("Car go to destination")

if __name__ == "__main__":  
    # controller = Controller(BROKER_URL, TOPIC_PUBLISH_A, TOPIC_SUBSCRIBE_A, TOPIC_PUBLISH_B, TOPIC_SUBSCRIBE_B,PATH_A, EDGE_A,PATH_B,EDGE_B )
    controller = Controller(BROKER_URL, TOPIC_PUBLISH_A, TOPIC_SUBSCRIBE_A, TOPIC_PUBLISH_B, TOPIC_SUBSCRIBE_B)
    try:
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        print("\nProgram stopped.")