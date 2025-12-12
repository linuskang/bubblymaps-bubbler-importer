#!/usr/bin/env python3
import json

INPUT_JSON_FILE = "./import_chunks/chunk_7.json"  # Path to your JSON file

def count_bubblers(file_path: str) -> int:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("data", [])
    # Count entries where "Fountain" is in the categories
    bubbler_count = sum(1 for entry in entries if "Fountain" in entry[1])
    return bubbler_count

if __name__ == "__main__":
    total_bubblers = count_bubblers(INPUT_JSON_FILE)
    print(f"Total water bubbler entries: {total_bubblers}")
