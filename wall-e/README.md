# Wall-E Topology Data Collection System

This module provides comprehensive topology data collection and analysis for gRPC metrics with `METRIC_TYPE_CONTROLLER`.

## Features

### 1. Service Collection (`service_collection`)
- **Type**: `map<string, int>`
- **Purpose**: Bidirectional mapping between service names and unique integer IDs
- **Example**: `{"auth-service": 1, "user-service": 2}`

### 2. Endpoint Collection (`endpoint_collection`)
- **Type**: `map<string, int>`
- **Purpose**: Bidirectional mapping between endpoint/link names and unique integer IDs
- **Example**: `{"/api/auth": 1, "/api/users": 2}`

### 3. Endpoint Mapping (`endpoint_mapping`)
- **Type**: `map<int, []int>`
- **Purpose**: Maps service IDs to their upstream link IDs
- **Logic**: `endpoint_mapping[service_id] = [upstream_link_ids...]`
- **Example**: `{5: [1, 2]}` means service ID 5 has upstream links 1 and 2

### 4. Connection Graph (`connection_graph`)
- **Type**: `map<int, map<int, []int>>`
- **Purpose**: Adjacency graph showing service connections through downstream links
- **Logic**: `connection_graph[upstream_service][downstream_service] = [downstream_links...]`
- **Example**: `{1: {3: [3], 4: [4]}}` means upstream service 1 connects to downstream service 3 via link 3, and to service 4 via link 4

### 5. Parent Collection (`parent_collection`)
- **Type**: `map<int, []int>`
- **Purpose**: Maps downstream links to their upstream links
- **Logic**: `parent_collection[downstream_link] = [upstream_links...]`
- **Example**: `{3: [1, 2]}` means downstream link 3 has upstream links 1 and 2

## Data Processing Logic

The system processes gRPC metrics with the following criteria:
1. `metric_type` must be `METRIC_TYPE_CONTROLLER`
2. All four topology fields must be non-null and non-empty:
   - `upstream_services`
   - `downstream_services`
   - `upstream_links`
   - `downstream_links`

### Processing Steps

1. **Service Mapping**: Assign unique IDs to all services
2. **Endpoint Mapping**: Assign unique IDs to all links/endpoints
3. **Service-to-Links**: Map services to their upstream links
4. **Connection Graph**: Build adjacency relationships
5. **Parent Collection**: Build link hierarchy

## Usage

### Automatic Processing
The system automatically processes incoming gRPC metrics:

```go
// In your gRPC handler
walle.GetInstance().ProcessMetric(metric)
```

### Manual Testing
```bash
TEST_MODE=true go run .
```

### Configuration
```go
manager := walle.GetInstance()
manager.SetSaveInterval(30 * time.Second)  // Save every 30 seconds
manager.SetFilename("custom_topology.json")  // Custom filename
```

## Output Format

The system saves data to `connection.json` with the following structure:

```json
{
  "service_collection": {"service_name": id},
  "service_reverse": {"id": "service_name"},
  "endpoint_collection": {"endpoint_name": id},
  "endpoint_reverse": {"id": "endpoint_name"},
  "endpoint_mapping": {"service_id": [upstream_link_ids]},
  "connection_graph": {"upstream_id": {"downstream_id": [link_ids]}},
  "parent_collection": {"downstream_link_id": [upstream_link_ids]},
  "service_counter": next_service_id,
  "endpoint_counter": next_endpoint_id
}
```

## Thread Safety

All operations are thread-safe with read-write mutexes protecting concurrent access.

## Persistence

- **Auto-save**: Every 10 seconds (configurable)
- **Graceful shutdown**: Saves on server termination
- **Load on startup**: Restores previous state if file exists 