# BGP Routing Simulator (Optimized)

## Overview
This project is a high-performance **BGP routing simulator** It simulates inter-domain routing behavior using real-world topology data from CAIDA.

The simulator:
- Builds an AS-level graph from CAIDA relationship data
- Seeds route announcements
- Propagates routes using BGP policies (up / across / down)
- Applies tie-breaking rules and optional ROV filtering
- Outputs final routing tables in ribs.csv format

## Constraints
Designed to operate under strict system limits:
- CPU: ≤ 2 cores
- Memory: ≤ 8 GB RAM

All design decisions prioritize performance, memory efficiency, and cache locality.

## How It Works (High-Level)
1. Graph Construction
   - Parses CAIDA relationship file
   - Builds AS graph with provider, customer, and peer links

2. Preprocessing
   - Removes duplicate edges
   - Detects cycles
   - Computes propagation ranks

3. Simulation
   - Each prefix simulated independently
   - Phases:
     - Up (customers → providers)
     - Across (peers)
     - Down (providers → customers)
   - Best route chosen via:
     - Relationship
     - Path length
     - Next-hop ASN

4. Output
   - Writes routes to ribs.csv

## Key Optimizations

### Data-Oriented Graph Layout
Flat arrays + offsets instead of nested vectors
- Better cache locality
- Less memory overhead

### Precomputed Strings
ASN strings and fragments built once
- Avoid repeated formatting

### Lightweight Route Records
Store only parent pointer + node index
- Reduces memory footprint

### Arena Allocation
Fixed-capacity route storage
- No dynamic allocation during runtime

### Epoch-Based Reset
Avoid clearing arrays
- O(1) logical reset

### Cache-Friendly Traversal
Nodes grouped by rank
- Improves cache usage

### Selective Processing
Only active nodes processed
- Avoids wasted work

### Optimized Peer Phase
Process only updated nodes
- Reduces full scans

### Load-Balanced Multithreading
Up to 2 threads
- Work split by cost

### Buffered I/O
Large output buffer (~8MB)
- Fewer system calls

## Usage

Option 1:
bgp_simulator --relationships <graph_file> --announcements <anns_csv> --rov-asns <rov_csv>

Option 2:
bgp_sim <dataset_dir> <output_csv>

Option 3:
bgp_sim <graph> <anns> <rov> <output>

## Output Format
asn,prefix,as_path

Example:
11537,10.0.0.0/24,"(11537, 557, 13335, ...)"

## Summary
Optimized for:
- Low memory usage
- High cache efficiency
- Minimal allocations
- Predictable performance

Demonstrates data-oriented design and systems-level optimization.
