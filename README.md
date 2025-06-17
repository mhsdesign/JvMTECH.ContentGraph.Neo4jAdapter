# JvMTECH.ContentGraph.Neo4jAdapter

Neo4j Content Graph Projection adapter for Neos CMS 9.0. This package provides an experimental alternative to the default Doctrine DBAL adapter by implementing the `ContentGraphProjectionInterface` using Neo4j as the underlying graph database.

## Installation

### 1. Install via Composer

```bash
composer require "jvmtech/contentgraph-neo4jadapter@dev"
```

### 2. Neo4j Database Setup

Ensure you have a Neo4j database instance running with **APOC (Awesome Procedures on Cypher)** installed. The adapter uses the `laudis/neo4j-php-client` library for connectivity and requires APOC procedures for advanced graph operations.

**Docker setup example with APOC:**
```bash
docker run \
    --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -d \
    -v neo4j_data:/data \
    -v neo4j_logs:/logs \
    -v neo4j_import:/var/lib/neo4j/import \
    -v neo4j_plugins:/plugins \
    --env NEO4J_AUTH=neo4j/neo4jneo4j \
    --env NEO4J_PLUGINS=["apoc"] \
    neo4j:latest
```

**Manual APOC installation:**
1. Download the APOC jar file from [Neo4j Labs](https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases)
2. Place it in your Neo4j `plugins/` directory
3. Restart Neo4j
4. Verify installation: `CALL apoc.help("apoc")`

## Configuration

### 1. Neo4j Connection Settings

```yaml
JvMTECH:
  ContentGraph:
    Neo4jAdapter:
      Connection:
        host: bolt://neo4j          # Neo4j bolt connection URL
        port: 7687                  # Neo4j bolt port (default: 7687)
        username: neo4j             # Neo4j username
        password: neo4jneo4j        # Neo4j password
```

### 2. Content Repository Configuration

```yaml
Neos:
  ContentRepositoryRegistry:
    presets:
      'default':
        contentGraphProjection:
          factoryObjectName: JvMTECH\ContentGraph\Neo4jAdapter\Neo4jContentGraphProjectionFactory
```

## Usage

Once configured, the Neo4j adapter will automatically handle:

- Content stream creation, forking, and removal
- Node creation and hierarchy management
- Workspace operations
- Subtree queries with position-based sorting
- Graph traversal and content retrieval

### Key Components

- **Neo4jContentGraphProjection**: Main projection class handling event processing
- **Neo4jContentGraph**: Content graph interface implementation
- **Neo4jContentSubgraph**: Subgraph queries and traversal with path-based transformation
- **NodeFactory**: Maps Neo4j results to Neos Node objects

## Development Status

⚠️ **Experimental Package**: This adapter is currently under heavy development. It currently does not support all event-handlers.

**Planned Features:**
- Complete event handler implementations
- Advanced relationship handling
- Performance optimizations

## Requirements

- Neos CMS 9.0+
- PHP 8.1+
- Neo4j 4.0+
- `laudis/neo4j-php-client` ^3.2.0

## Contributing

This is an experimental package. Contributions and feedback are welcome as we work towards a stable Neo4j content graph implementation.

## License

[Specify your license here]