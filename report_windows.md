# HarborX Window-Size Benchmark Report

- Total rows: **8,000,000** (16×500,000)  
  Update ratio: **0.50** → keyspace≈**4,000,000**
- Workers: **4**, chunk: **200,000**

- Arrow write (all parts): **26.056s**, SQLite append (all parts): **80.302s**

## Window W = 1
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 33.393s | 1.850s | **35.243s** |
| SQLite | 51.050s | 6.438s | **57.488s** |

## Window W = 4
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 19.191s | 0.909s | **20.101s** |
| SQLite | 42.619s | 11.303s | **53.922s** |

## Window W = 16
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 1.183s | 3.588s | **4.771s** |
| SQLite | 12.454s | 39.141s | **51.595s** |
