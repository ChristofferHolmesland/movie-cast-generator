# Execution times

Time unit is seconds unless specified.

The Python version of the program was executed on a machine running Windows 10 Pro with the following hardware:
- CPU: Intel i5-8600K 3.6 GHz
- Memory: 16 GB DDR4 2667 MHz
- Disk: SSD with read speed 500MB/s and write speed 320MB/s

## Pre-processing

The following timings were calculated using the [time(1)](https://linux.die.net/man/1/time) command.

| Task             | Python | MRJob / Spark |
| ----             | ------ | ------------- |
| name_basics      | 25.5   | 102.3 |
| title_basics     | 17.5   | 89.6 |
| title_principals | 32.9   | 664.3 |
| title_ratings    | 2.0    | 67.0 |
| graph            | 106.6  | 205.3 |
| genre_score      | 195.2  | 232.3 |
| total            | 379.7  | 1360.8 |

Reasons for the difference:
- The Python implementation is able to keep all of the data in memory.
- There is a significant overhead when connecting to Spark (typically 45-60 seconds).

## Algorithm

The following timings were calculated using the `time` function from the `time` library in Python.

### Interstallar

| Task                  | Python           | MRJob / Spark |
| ----                  | ------           | ------------- |
| candidate actors      | 60.3             | 331.5 |
| similarity score      | 89.6             | 448.8 |
| relation score        | 438.0 \*(1)      | 0.0362 \*(1)  |
| total                 | 13.7 days \*(2)  | 1182.1 |

\*(1) Time per relation score

\*(2) Estimated from the time per relation score