# DAG Pipeline Example

This example demonstrates the DAG (Directed Acyclic Graph) capabilities of PipeLink, allowing you to create complex data processing workflows with multiple parallel branches and explicit dependencies.

## Pipeline Visualization

The pipeline in this example has the following structure:

```
                   ┌─────────────────┐
                   │  generate_data  │
                   └─────────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │ clean_data  │ │   analyze   │ │   create    │
    │             │ │ statistics  │ │visualizations│
    └─────────────┘ └─────────────┘ └─────────────┘
              │            │            │
              │            │            │
              └────────┐   │   ┌────────┘
                       ▼   ▼   ▼
                   ┌─────────────────┐
                   │  prepare_report │
                   └─────────────────┘
                           │
                           ▼
                   ┌─────────────────┐
                   │generate_dashboard│
                   └─────────────────┘
```

## Running the Example

To run this example pipeline:

```bash
cd examples/dag_pipeline
python -m pipelink.python.pipelink run pipeline.yml
```

To visualize the pipeline DAG:

```bash
python -m pipelink.python.pipelink visualize pipeline.yml
```

## Pipeline Structure

This pipeline demonstrates several key DAG features:

1. **Multiple Parallel Branches**: After data generation, the pipeline splits into three parallel branches
2. **Explicit Dependencies**: Each node explicitly declares which nodes it depends on
3. **Multi-Input Merging**: The prepare_report node takes inputs from multiple upstream nodes
4. **Branch Merging**: The final generate_dashboard node combines the results from multiple paths

## Key Concepts

- **`depends_on`**: Explicit dependency declarations that define the pipeline structure
- **Parallel Processing**: Multiple nodes can run in parallel when they don't depend on each other
- **Input/Output Relationships**: Data still flows between nodes via named datasets
- **Execution Order**: The pipeline runner automatically determines the correct execution order

## Implementation Details

The pipeline uses DuckDB and Apache Arrow for efficient data transfer between nodes, even when they're written in different programming languages. 