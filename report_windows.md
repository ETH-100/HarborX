# HarborX Window-Size Benchmark Report

- Total rows: **8,000,000** (16×500,000)  
  Update ratio: **0.50** → keyspace≈**4,000,000**
- Workers: **4**, chunk: **200,000**

- Arrow write (all parts): **36.338s**, SQLite append (all parts): **79.344s**

## Window W = 1

| Path   | Compact (base) | Live query | TTF (base+live) |
|--------|---------------:|-----------:|----------------:|
| Arrow  |         10.115s|      0.621s|        **10.736s** |
| SQLite |         47.790s|      5.065s|        **52.855s** |

## Window W = 4

| Path   | Compact (base) | Live query | TTF (base+live) |
|--------|---------------:|-----------:|----------------:|
| Arrow  |         10.643s|      0.868s|        **11.511s** |
| SQLite |         32.504s|     10.941s|        **43.445s** |

## Window W = 16

| Path   | Compact (base) | Live query | TTF (base+live) |
|--------|---------------:|-----------:|----------------:|
| Arrow  |          1.329s|      3.202s|         **4.531s** |
| SQLite |          3.081s|     34.356s|        **37.438s** |
