#!/usr/bin/env python3
"""
Collision-free Start -> Load -> Unload -> Destination planning for 2 AGVs

- One-to-one assignments for LOAD, UNLOAD, DESTINATION.
- Optimizes makespan over the full chain (speed-aware).
- Uses original dijkstra(), calculate_travel_time(), adjust_path_timing(), check_path_collision().
- Prints only (no file output).
"""

import sys
from itertools import permutations, product
from typing import Dict, List, Tuple, Optional
from collision_avoidance import MultiAGVPlanner 

Node = str | int

class ProblemBPlanner(MultiAGVPlanner):
    def __init__(self, map_url: str, agv_speeds: Optional[Dict[str, float]] = None):
        super().__init__(map_url, agv_speeds or {})
        self.load_nodes: List[Node] = []
        self.unload_nodes: List[Node] = []

    # ---- Build and index node types (reusing base) ----
    def build_adjacency_list(self):
        super().build_adjacency_list()
        # Index LOAD/UNLOAD from the fetched JSON
        for node in self.graph_data.get("nodes", []):
            t = str(node.get("type", "")).upper()
            nid = node["id"]
            if t == "LOAD":
                self.load_nodes.append(nid)
            elif t == "UNLOAD":
                self.unload_nodes.append(nid)

    # ---- Utilities ----
    @staticmethod
    def _concat_path(p1: List[Node], p2: List[Node]) -> List[Node]:
        """Concatenate two paths, avoiding duplicate junction node."""
        if not p1:
            return list(p2)
        if not p2:
            return list(p1)
        return p1 + (p2[1:] if p1[-1] == p2[0] else p2)

    def _full_chain_distance(
        self, s: Node, l: Node, u: Node, d: Node
    ) -> Tuple[List[Node], List[Node], List[Node], float]:
        """
        Return (pathSL, pathLU, pathUD, total_distance). If any leg unreachable, distance = inf.
        """
        pSL, dSL = self.dijkstra(s, l)
        pLU, dLU = self.dijkstra(l, u)
        pUD, dUD = self.dijkstra(u, d)

        if any(dist == float("inf") for dist in (dSL, dLU, dUD)):
            return pSL, pLU, pUD, float("inf")
        return pSL, pLU, pUD, dSL + dLU + dUD

    # ---- Optimize assignments ----
    def _optimize_full_assignments(
        self, starts: List[Node], loads: List[Node], unloads: List[Node], dests: List[Node]
    ) -> Dict[Node, Tuple[Node, Node, Node]]:
        """
        Returns mapping: start -> (load, unload, dest) that minimizes makespan over full chain.
        All three target sets are used one-to-one.
        """
        assert len(starts) == len(loads) == len(unloads) == len(dests) == 2, \
            "This optimizer assumes exactly 2 of each."

        best_assign: Optional[Dict[Node, Tuple[Node, Node, Node]]] = None
        best_makespan = float("inf")

        # All bijections for loads, unloads, destinations (relative to starts order)
        for permL in permutations(loads, 2):
            for permU in permutations(unloads, 2):
                for permD in permutations(dests, 2):
                    # Evaluate makespan (speed-aware)
                    times = []
                    feasible = True
                    for i, s in enumerate(starts):
                        l = permL[i]
                        u = permU[i]
                        d = permD[i]

                        # total distance across the three legs
                        _, _, _, total_dist = self._full_chain_distance(s, l, u, d)
                        if total_dist == float("inf"):
                            feasible = False
                            break

                        agv_id = f"AGV{i+1}"
                        t = self.calculate_travel_time(total_dist, agv_id)
                        times.append(t)

                    if not feasible:
                        continue

                    makespan = max(times) if times else float("inf")
                    if makespan < best_makespan:
                        best_makespan = makespan
                        best_assign = {starts[i]: (permL[i], permU[i], permD[i]) for i in range(2)}

        if not best_assign:
            raise ValueError("No feasible full assignments found (check connectivity).")
        return best_assign

    # ---- Scheduling with collision avoidance ----
    def _schedule_collision_free(
        self, chain_paths: Dict[str, List[Node]],
        max_stagger: float = 10.0,
        step: float = 0.5
    ) -> Tuple[Dict[str, List[Tuple[Node, float]]], Optional[str]]:
        """
        Schedule AGVs with adjust_path_timing, staggering AGV2 start if needed to avoid collisions.
        Returns (scheduled_paths, warning_message_if_any).
        """
        # Schedule AGV1 at t=0
        agv1_id, agv2_id = "AGV1", "AGV2"
        scheduled: Dict[str, List[Tuple[Node, float]]] = {}

        sp1 = self.adjust_path_timing(chain_paths[agv1_id], 0.0, agv1_id, scheduled)
        scheduled[agv1_id] = sp1

        # Try AGV2 with increasing start offsets until no collisions with AGV1
        delay = 0.0
        warning = None
        while delay <= max_stagger:
            sp2 = self.adjust_path_timing(chain_paths[agv2_id], delay, agv2_id, scheduled)
            # Check collisions
            collisions = self.check_path_collision(scheduled[agv1_id], sp2)
            if not collisions:
                scheduled[agv2_id] = sp2
                break
            delay += step
        else:
            # If we exit normally (no break), keep last attempt and report
            scheduled[agv2_id] = sp2
            warning = "Warning: could not eliminate all collisions within stagger window."

        return scheduled, warning

    # ---- Public: build the full collision-free plan and print ----
    def run(self):
        starts = list(self.start_nodes)
        loads = list(self.load_nodes)
        unloads = list(self.unload_nodes)
        dests = list(self.destination_nodes)

        # Optimize one-to-one full chain assignments
        full_assign = self._optimize_full_assignments(starts, loads, unloads, dests)

        # Build leg paths and full chains per AGV
        chains: Dict[str, List[Node]] = {}
        details = []  # For pretty printing

        for i, s in enumerate(starts, start=1):
            agv = f"AGV{i}"
            l, u, d = full_assign[s]

            pSL, dSL = self.dijkstra(s, l)
            pLU, dLU = self.dijkstra(l, u)
            pUD, dUD = self.dijkstra(u, d)

            if any(dist == float("inf") for dist in (dSL, dLU, dUD)):
                raise ValueError(f"No path for {agv} across one of the legs")

            full_path = self._concat_path(self._concat_path(pSL, pLU), pUD)
            chains[agv] = full_path

            total_dist = dSL + dLU + dUD
            total_time = self.calculate_travel_time(total_dist, agv)

            details.append({
                "agv": agv,
                "start": s, "load": l, "unload": u, "dest": d,
                "pathSL": pSL, "pathLU": pLU, "pathUD": pUD,
                "distSL": dSL, "distLU": dLU, "distUD": dUD,
                "total_dist": total_dist, "total_time": total_time
            })

        # Schedule collision-free (stagger AGV2 if needed)
        scheduled_paths, warn = self._schedule_collision_free(chains)

        # Final collision check for transparency
        collisions = self.check_path_collision(scheduled_paths["AGV1"], scheduled_paths["AGV2"])

        # ---- Print results ----
        print("\nStart → Load → Unload → Destination")
        print("=" * 70)
        for d in details:
            print(f"{d['agv']}: {d['start']} → {d['load']} → {d['unload']} → {d['dest']}")
            print("  Path Start->Load:    ", " → ".join(map(str, d["pathSL"])))
            print("  Path Load->Unload:   ", " → ".join(map(str, d["pathLU"])))
            print("  Path Unload->Dest:   ", " → ".join(map(str, d["pathUD"])))
            print(f"  Distances: SL={d['distSL']:.2f}, LU={d['distLU']:.2f}, UD={d['distUD']:.2f}  |  Total={d['total_dist']:.2f}")
            print(f"  Travel Time (base):  {d['total_time']:.2f} units")
            print("-" * 70)

        if warn:
            print(warn)
        if collisions:
            print("\nRemaining collision warnings:")
            for node, t1, t2 in collisions:
                print(f"  - Node {node}: AGV1 at {t1:.2f}, AGV2 at {t2:.2f}")
        else:
            print("\nNo collisions detected between AGV1 and AGV2.")

        print("=" * 70)

if __name__ == "__main__":
    # Change URL if needed
    # url = "http://127.0.0.1:5500/ProblemB/objmap_v0.0.0.json"
    url = "http://127.0.0.1:5500/One_Controller/map_v2.0.0_250812.json"
    planner = ProblemBPlanner(url)

    print(f"Fetching map data from {url}...")
    if not planner.fetch_map_data():
        sys.exit("Failed to fetch map data")

    print("Building graph...")
    planner.build_adjacency_list()
    print(f"START nodes: {planner.start_nodes}")
    print(f"LOAD nodes: {planner.load_nodes}")
    print(f"UNLOAD nodes: {planner.unload_nodes}")
    print(f"DESTINATION nodes: {planner.destination_nodes}")

    try:
        planner.run()
    except ValueError as e:
        sys.exit(f"Error: {e}")
