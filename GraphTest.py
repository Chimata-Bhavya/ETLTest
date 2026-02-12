import json
import os
from pathlib import Path
from pyvis.network import Network
from collections import defaultdict

DATA_FOLDER = r"C:\Users\Chimata.Charita\Downloads\Trial_Copy\Test"
ROOT_JOB_NAME = "GENDER_KTR"
OUTPUT_HTML = r"C:\Users\Chimata.Charita\Downloads\Trial_Copy\graphtest.html"


ALL_STEP_NODES = set()


def load_json_file(name: str, verbose: bool = False):
    candidates = [
        f"{name}.json",
        f"{name}_unified_execution_flow.json",
        f"{name}.kjb.json",
        f"{name}.ktr.json",
        name,
        f"{name}_entry.json",
    ]

    for fname in candidates:
        path = os.path.join(DATA_FOLDER, fname)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if verbose:
                        print(f"Loaded: {fname}")
                    return data, fname
            except Exception as e:
                print(f"Error loading {fname}: {e}")
                continue

    print(f"No JSON file found for: {name}")
    return None, None


class ETLGraphBuilder:
    def __init__(self, data_folder: str):
        self.data_folder = Path(data_folder)
        self.nodes = {}     # node_name → {name, type, properties, filename}
        self.edges = []     # list of edge dicts

    def build_from_json(self, root_name: str):
        """Load root JSON → extract hops → load properties for all nodes."""
        print(f"\n🚀 Building graph from JSON root: {root_name}")

        # 1. Load the root JSON (should contain hops)
        root_data, root_file = load_json_file(root_name, verbose=True)
        if not root_data:
            raise FileNotFoundError(f"Root JSON not found: {root_name}")

        # 2. Extract hops (assume standard location: execution_flow.hops)
        hops = root_data.get("execution_flow", {}).get("hops", [])
        print(f"Found {len(hops)} hops in root JSON")

        self.edges = []
        all_nodes = set()

        for hop in hops:
            fr = hop.get("from", "").strip()
            to = hop.get("to", "").strip()
            if not fr or not to:
                continue

            enabled = hop.get("enabled", "Y").upper() == "Y"
            hop_type = hop.get("type", "Unconditional")

            edge = {
                "from": fr,
                "to": to,
                "type": "HOP",
                "hop_type": hop_type,
                "enabled": "Y" if enabled else "N",
            }

            self.edges.append(edge)
            all_nodes.add(fr)
            all_nodes.add(to)

        # 3. Load properties for every node found
        print(f"📦 Loading properties for {len(all_nodes)} nodes...")
        for node_name in all_nodes:
            self._load_node_properties(node_name)

        print(f"✅ Graph built: {len(self.nodes)} nodes | {len(self.edges)} edges")

    def _load_node_properties(self, node_name: str):
        if node_name in self.nodes:
            return

        data, filename = load_json_file(node_name)
        if data:
            props = {
                "name": data.get("Name", node_name),
                "type": data.get("type", "unknown"),
                "properties": data,  # full JSON content
                "filename": filename or "unknown"
            }
            # Collect steps
            if data.get("is_step", False):
                ALL_STEP_NODES.add(node_name)
        else:
            props = {
                "name": node_name,
                "type": "unknown",
                "properties": {},
                "filename": "NO_FILE"
            }

        self.nodes[node_name] = props

    def visualize(self):
        net = Network(height="1000px", width="100%", directed=True, notebook=False)
        net.set_options(
            '{"physics": {"enabled": false},'
            '"nodes": {"shape": "box", "font": {"size": 12, "bold": true}},'
            '"edges": {"arrows": "to"}}'
        )

        print(f"📦 Adding {len(self.nodes)} nodes to PyVis...")

        default_color = "#9e5d8f"

        for node_id, props in self.nodes.items():
            tooltip_lines = [
                f"Name: {props['name']}",
                f"Type: {props['type']}",
                f"File: {props['filename']}",
                ""
            ]

            data = props["properties"]

            # Show SQL if present
            if "sql" in data and data["sql"]:
                tooltip_lines.append(f"SQL: {data['sql']}")

            # Show connection if present
            if "connection" in data:
                tooltip_lines.append(f"Connection: {data['connection']}")

            tooltip_lines.append("Full Properties:")
            json_str = json.dumps(data, indent=2, default=str)
            tooltip_lines.extend(json_str.split('\n'))

            net.add_node(
                node_id,
                label=props["name"][:30],
                color=default_color,
                size=35,
                title="\n".join(tooltip_lines),
                shape="box",
            )

        # Edges
        safe_count = 0
        for edge in self.edges:
            fr = edge["from"]
            to = edge["to"]
            if fr in self.nodes and to in self.nodes:
                color = "#ca9f14" if edge["hop_type"] == "Unconditional" else "#ef4444"

                net.add_edge(
                    fr,
                    to,
                    color=color,
                    width=4,
                    dashes=(edge["enabled"] != "Y"),
                    label=edge["type"],
                    title=f"Enabled: {edge['enabled']}",
                )
                safe_count += 1

        print(f"🔗 Added {safe_count}/{len(self.edges)} edges")
        net.write_html(OUTPUT_HTML)
        print(f"✅ Graph saved: {OUTPUT_HTML}")

    def export_json(self, filename="etl_graph.json"):
        export_data = {
            "nodes": [
                {
                    "id": nid,
                    "name": p["name"],
                    "type": p["type"],
                    "filename": p["filename"],
                    "properties": p["properties"]
                }
                for nid, p in self.nodes.items()
            ],
            "edges": self.edges
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, default=str)
        print(f"✅ Exported: {filename}")


# MAIN
if __name__ == "__main__":
    print(f"Processing root: {ROOT_JOB_NAME}")

    builder = ETLGraphBuilder(DATA_FOLDER)
    builder.build_from_json(ROOT_JOB_NAME)

    builder.export_json()
    builder.visualize()

    print(f"\n🎉 Graph complete: {len(builder.nodes)} nodes, {len(builder.edges)} edges")
    print(f"Steps found: {sorted(ALL_STEP_NODES)}")
    print(f"Open → {OUTPUT_HTML}")