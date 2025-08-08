# HarborX Window-Size Benchmark Report

- Total rows: **8,000,000** (16×500,000)  
  Update ratio: **0.50** → keyspace≈**4,000,000**
- Workers: **4**, chunk: **200,000**

- Arrow write (all parts): **19.859s**, SQLite append (all parts): **77.029s**

## Window W = 1
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 9.783s | 0.514s | **10.297s** |
| SQLite | 43.546s | 5.201s | **48.747s** |

## Window W = 4
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 8.643s | 0.890s | **9.533s** |
| SQLite | 33.498s | 11.055s | **44.552s** |

## Window W = 16
| Path | Compact (base) | Live query | TTF (base+live) |
|---|---:|---:|---:|
| Arrow | 0.513s | 3.457s | **3.970s** |
| SQLite | 4.269s | 36.765s | **41.034s** |
