#!/usr/bin/env python3
"""
Multi-AGV Path Planning with Assignment Optimization
Optimally assigns multiple AGVs to destinations to minimize total distance
"""

import requests
import json
import heapq
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import argparse
import sys
from itertools import permutations

class MultiAGVPlanner:
    def __init__(self, map_url: str, agv_speeds: Optional[Dict[str, float]] = None):
        self.map_url = map_url
        self.graph_data = None
        self.adjacency_list = {}
        self.start_nodes = []
        self.destination_nodes = []
        self.distance_cache = {}  # Cache for kstradij results
        # AGV speeds in units per second (default: 1.0 for all AGVs)
        self.agv_speeds = agv_speeds or {}
        self.default_speed = 1.0  # Default speed if not specified
        # For collision avoidance
        self.agv_schedules = {}  # {agv_id: [(node, arrival_time, departure_time), ...]}
        self.node_occupation_timeline = {}  # {node: [(agv_id, start_time, end_time), ...]}

    def fetch_map_data(self) -> bool:
        """Fetch map data from the API"""
        try:
            response = requests.get(self.map_url)
            response.raise_for_status()
            self.graph_data = response.json()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error fetching map data: {e}")
            return False

    def build_adjacency_list(self):
        """Convert graph data to adjacency list for pathfinding"""
        if not self.graph_data or "edges" not in self.graph_data:
            raise ValueError("No graph data available")

        # Initialize adjacency list and identify start/destination nodes
        nodes = self.graph_data.get("nodes", [])
        for node in nodes:
            node_id = node["id"]
            self.adjacency_list[node_id] = []

            # Identify node types
            if node.get("type") == "START":
                self.start_nodes.append(node_id)
            elif node.get("type") == "DESTINATION":
                self.destination_nodes.append(node_id)

        # Build adjacency list from edges
        edges = self.graph_data["edges"]
        for edge in edges:
            source = edge["source"]
            target = edge["target"]
            weight = edge.get("weight", 1.0)

            # Add bidirectional edges
            self.adjacency_list[source].append((target, weight))
            self.adjacency_list[target].append((source, weight))

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
        visited = set()

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

    def compute_distance_matrix(
        self, starts: List[str], destinations: List[str]
    ) -> Dict[Tuple[str, str], Tuple[List[str], float]]:
        """Compute all pairwise distances between starts and destinations"""
        matrix = {}
        for start in starts:
            for dest in destinations:
                path, distance = self.dijkstra(start, dest)
                matrix[(start, dest)] = (path, distance)
        return matrix

    def get_agv_speed(self, agv_id: str) -> float:
        """Get speed for a specific AGV"""
        return self.agv_speeds.get(agv_id, self.default_speed)

    def calculate_travel_time(self, distance: float, agv_id: str) -> float:
        """Calculate travel time based on distance and AGV speed"""
        speed = self.get_agv_speed(agv_id)
        return distance / speed if speed > 0 else float('inf')

    def find_optimal_assignment(self, starts: List[str], destinations: List[str]) -> Tuple[Dict[str, str], float]:
        """
        Find optimal assignment of AGVs to destinations considering speed
        Returns: (assignment dict, total time)
        """
        if len(starts) != len(destinations):
            raise ValueError(f"Number of starts ({len(starts)}) must equal destinations ({len(destinations)})")

        # Compute distance matrix
        distance_matrix = self.compute_distance_matrix(starts, destinations)

        # For 2 AGVs, there are only 2 possible assignments
        if len(starts) == 2:
            # Assignment 1: start[0]->dest[0], start[1]->dest[1]
            time1_agv1 = self.calculate_travel_time(distance_matrix[(starts[0], destinations[0])][1], "AGV1")
            time1_agv2 = self.calculate_travel_time(distance_matrix[(starts[1], destinations[1])][1], "AGV2")
            total_time1 = max(time1_agv1, time1_agv2)  # Total time is the maximum since AGVs move in parallel

            # Assignment 2: start[0]->dest[1], start[1]->dest[0]
            time2_agv1 = self.calculate_travel_time(distance_matrix[(starts[0], destinations[1])][1], "AGV1")
            time2_agv2 = self.calculate_travel_time(distance_matrix[(starts[1], destinations[0])][1], "AGV2")
            total_time2 = max(time2_agv1, time2_agv2)

            if total_time1 <= total_time2:
                return {starts[0]: destinations[0], starts[1]: destinations[1]}, total_time1
            else:
                return {starts[0]: destinations[1], starts[1]: destinations[0]}, total_time2

        # For larger problems, try all permutations
        best_assignment = None
        best_time = float("inf")

        for perm in permutations(destinations):
            assignment = {}
            max_time = 0  # Track the maximum time among all AGVs

            for i, start in enumerate(starts):
                dest = perm[i]
                assignment[start] = dest
                agv_id = f"AGV{i+1}"
                distance = distance_matrix[(start, dest)][1]
                travel_time = self.calculate_travel_time(distance, agv_id)
                max_time = max(max_time, travel_time)

            if max_time < best_time:
                best_time = max_time
                best_assignment = assignment

        return best_assignment, best_time

    def check_path_collision(self, path1: List[Tuple[str, float]], path2: List[Tuple[str, float]]) -> List[Tuple[str, float, float]]:
        """
        Check for collisions between two AGV paths with timestamps
        Returns list of (node, time1, time2) where collisions occur
        """
        collisions = []
        
        # Create time intervals for each node in both paths
        node_times1 = {}
        node_times2 = {}
        
        # Assume AGVs spend 0.5 time units at each node
        node_duration = 0.5
        
        for node, arrival_time in path1:
            node_times1[node] = (arrival_time, arrival_time + node_duration)
            
        for node, arrival_time in path2:
            node_times2[node] = (arrival_time, arrival_time + node_duration)
            
        # Check for overlapping time intervals at same nodes
        for node in set(node_times1.keys()) & set(node_times2.keys()):
            start1, end1 = node_times1[node]
            start2, end2 = node_times2[node]
            
            # Check if time intervals overlap
            if not (end1 <= start2 or end2 <= start1):
                collisions.append((node, start1, start2))
                
        return collisions

    def adjust_path_timing(self, path: List[str], start_time: float, agv_id: str, other_schedules: Dict[str, List[Tuple[str, float]]]) -> List[Tuple[str, float]]:
        """
        Adjust path timing to avoid collisions with other AGVs
        Returns list of (node, arrival_time) tuples
        """
        timed_path = []
        current_time = start_time
        
        for i, node in enumerate(path):
            if i > 0:
                # Calculate travel time from previous node
                prev_node = path[i-1]
                distance = 0
                for neighbor, weight in self.adjacency_list[prev_node]:
                    if neighbor == node:
                        distance = weight
                        break
                travel_time = self.calculate_travel_time(distance, agv_id)
                current_time += travel_time
            
            # Check if node is occupied by another AGV at this time
            conflict_found = True
            wait_time = 0
            max_wait = 10.0  # Maximum wait time to prevent infinite loops
            
            while conflict_found and wait_time < max_wait:
                conflict_found = False
                for other_agv, other_path in other_schedules.items():
                    for other_node, other_time in other_path:
                        # Check if same node at overlapping time (with 0.5 buffer)
                        if other_node == node and abs(current_time + wait_time - other_time) < 0.5:
                            conflict_found = True
                            wait_time += 0.5
                            break
                if conflict_found:
                    break
                    
            timed_path.append((node, current_time + wait_time))
            current_time += wait_time
            
        return timed_path

    def generate_multi_agv_plan(self, agv_assignments: Optional[Dict[str, str]] = None) -> Dict:
        """
        Generate movement plan for multiple AGVs
        If agv_assignments is None, will optimize assignment automatically
        """
        if agv_assignments is None:
            # Auto-detect and optimize
            if len(self.start_nodes) != 2 or len(self.destination_nodes) != 2:
                raise ValueError(
                    f"Expected 2 start nodes and 2 destinations, got {len(self.start_nodes)} and {len(self.destination_nodes)}"
                )

            # Find optimal assignment
            assignment, total_time = self.find_optimal_assignment(self.start_nodes, self.destination_nodes)
            print(f"Optimal assignment found with total completion time: {total_time:.2f} time units")
        else:
            assignment = agv_assignments
            total_distance = 0

        # Build distance matrix for assigned pairs
        distance_matrix = self.compute_distance_matrix(list(assignment.keys()), list(assignment.values()))

        # Generate plan
        plan = {
            "map_id": self.graph_data.get("id", "unknown") if self.graph_data else "unknown",
            "agv_count": len(assignment),
            "total_distance": 0,
            "total_time": 0,
            "agv_plans": [],
            "timestamp": datetime.now().isoformat(),
            "agv_speeds": self.agv_speeds,
            "collision_avoidance_enabled": True,
        }

        # Create individual AGV plans with collision avoidance
        agv_id = 1
        scheduled_paths = {}  # Store scheduled paths for collision checking
        max_completion_time = 0
        
        for start, dest in assignment.items():
            path, distance = distance_matrix[(start, dest)]
            agv_name = f"AGV{agv_id}"
            
            # Calculate base travel time
            travel_time = self.calculate_travel_time(distance, agv_name)
            
            # Adjust path timing to avoid collisions
            timed_path = self.adjust_path_timing(path, 0, agv_name, scheduled_paths)
            scheduled_paths[agv_name] = timed_path
            
            # Calculate actual completion time (last node arrival time)
            completion_time = timed_path[-1][1] if timed_path else 0
            max_completion_time = max(max_completion_time, completion_time)
            
            agv_plan = {
                "agv_id": agv_name,
                "start_node": start,
                "destination": dest,
                "speed": self.get_agv_speed(agv_name),
                "movements": [{
                    "from": start,
                    "to": dest,
                    "path": path,
                    "distance": distance,
                    "travel_time": travel_time,
                    "scheduled_path": timed_path,
                }],
                "total_distance": distance,
                "total_time": completion_time,
            }

            plan["agv_plans"].append(agv_plan)
            plan["total_distance"] += distance
            agv_id += 1
            
        plan["total_time"] = max_completion_time
        
        # Check for any remaining collisions
        collision_report = []
        agv_names = list(scheduled_paths.keys())
        for i in range(len(agv_names)):
            for j in range(i + 1, len(agv_names)):
                collisions = self.check_path_collision(
                    scheduled_paths[agv_names[i]],
                    scheduled_paths[agv_names[j]]
                )
                if collisions:
                    collision_report.append({
                        "agv1": agv_names[i],
                        "agv2": agv_names[j],
                        "collisions": collisions
                    })
        
        if collision_report:
            plan["collision_warnings"] = collision_report

        return plan

def main():
    parser = argparse.ArgumentParser(description="Multi-AGV Path Planning Tool with Speed Optimization")
    parser.add_argument(
        "--url",
        default="https://hackathon.omelet.tech/api/maps/980778ba-4ce1-4094-81d8-aa1f6da40b93/",
        help="Map API URL",
    )
    parser.add_argument("--output", default="multi_agv_plan.json", help="Output file name")
    parser.add_argument("--format", choices=["json", "txt"], default="json", help="Output format")
    parser.add_argument(
        "--agv-speeds",
        help="AGV speeds in format 'AGV1:speed1,AGV2:speed2' (default: 1.0 for all)",
        default="",
    )

    args = parser.parse_args()

    # Parse AGV speeds
    agv_speeds = {}
    if args.agv_speeds:
        for speed_spec in args.agv_speeds.split(","):
            if ":" in speed_spec:
                agv_id, speed = speed_spec.split(":")
                try:
                    agv_speeds[agv_id.strip()] = float(speed.strip())
                except ValueError:
                    print(f"Warning: Invalid speed value for {agv_id}, using default")

    # Create planner with speed configuration
    planner = MultiAGVPlanner(args.url, agv_speeds)

    # Fetch map data
    print(f"Fetching map data from {args.url}...")
    if not planner.fetch_map_data():
        print("Failed to fetch map data")
        sys.exit(1)

    # Build graph
    print("Building graph...")
    try:
        planner.build_adjacency_list()
    except ValueError as e:
        print(f"Error building graph: {e}")
        sys.exit(1)

    print(f"Found {len(planner.start_nodes)} start nodes: {planner.start_nodes}")
    print(f"Found {len(planner.destination_nodes)} destination nodes: {planner.destination_nodes}")

    # Check if manual assignment provided
    manual_assignment = None
    print("Optimizing AGV assignment automatically...")

    # Generate multi-AGV plan
    try:
        plan = planner.generate_multi_agv_plan(manual_assignment)
    except ValueError as e:
        print(f"Error generating plan: {e}")
        sys.exit(1)
    
    # Save results
    if args.format == "json":
        with open(args.output, "w") as f:
            json.dump(plan, f, indent=2)
    else:  # txt format
        with open(args.output, "w") as f:
            f.write("Multi-AGV Movement Plan with Speed Optimization\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Map ID: {plan['map_id']}\n")
            f.write(f"AGV Count: {plan['agv_count']}\n")
            f.write(f"Total Combined Distance: {plan['total_distance']:.2f}\n")
            f.write(f"Total Completion Time: {plan['total_time']:.2f} time units\n")
            f.write(f"Collision Avoidance: {'Enabled' if plan['collision_avoidance_enabled'] else 'Disabled'}\n")
            f.write(f"Generated: {plan['timestamp']}\n\n")

            for agv in plan["agv_plans"]:
                f.write(f"\n{agv['agv_id']}:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Start: {agv['start_node']} → Destination: {agv['destination']}\n")
                f.write(f"Speed: {agv['speed']:.2f} units/sec\n")
                f.write(f"Distance: {agv['total_distance']:.2f}\n")
                f.write(f"Completion Time: {agv['total_time']:.2f} time units\n")
                for movement in agv["movements"]:
                    f.write(f"Path: {' → '.join(movement['path'])}\n")
                    if "scheduled_path" in movement:
                        f.write("Scheduled Times:\n")
                        for node, time in movement["scheduled_path"]:
                            f.write(f"  - {node}: arrival at {time:.2f}\n")

    print(f"\nMulti-AGV plan saved to {args.output}")
    print(f"Total combined distance: {plan['total_distance']:.2f}")
    print(f"Total completion time: {plan['total_time']:.2f} time units")

    # Display summary
    # print("\nAssignment Summary:")
    # for agv in plan["agv_plans"]:
    #     print(f"  {agv['agv_id']}: {agv['start_node']} → {agv['destination']}")
    #     print(f"    - Distance: {agv['total_distance']:.2f}")
    #     print(f"    - Speed: {agv['speed']:.2f} units/sec")
    #     print(f"    - Completion time: {agv['total_time']:.2f} time units")
    
    # Display collision warnings if any
    if "collision_warnings" in plan:
        print("\nCollision Warnings:")
        for warning in plan["collision_warnings"]:
            print(f"  Potential collision between {warning['agv1']} and {warning['agv2']}:")
            for node, time1, time2 in warning["collisions"]:
                print(f"    - At node {node}: {warning['agv1']} at {time1:.2f}, {warning['agv2']} at {time2:.2f}")

if __name__ == "__main__":
    main()
