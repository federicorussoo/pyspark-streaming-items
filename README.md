# Frequent Items Mining in Data Streams with PySpark

PySpark Streaming application that compares two distinct approximation algorithms (**Sticky Sampling** and **Count-Min Sketch**) for identifying frequent items within continuous data streams. This project processes simulated item streams in real-time to compute and evaluate the estimated frequencies of items against an exact count baseline.

## Core Algorithms

The core objective is to identify elements in a stream of length `n` that occur with a frequency greater than or equal to a defined threshold `phi * n`. The solution implements two methods:

*   **Sticky Sampling:** A sampling-based approach that maintains a bounded dictionary of items. An item is tracked with a probability `p = r/n`, where `r` is a function of the accuracy (`epsilon`) and confidence (`delta`) parameters. It successfully isolates frequent and "almost frequent" items while severely restricting the memory footprint.
*   **Count-Min Sketch:** A sub-linear space data structure representing the data stream as a `d x w` matrix (where `d` is the number of hash functions and `w` is the number of columns). It guarantees no false negatives while maintaining high efficiency. The implementation utilizes a 2-universal family of hash functions to accurately map elements.

## Usage

### Prerequisites
* Python 3.x
* Apache Spark (PySpark)

### Execution
The script expects a continuous stream of integer items emitted via socket on a specified port. You can run the application via the command line by providing the required algorithmic parameters. See the example below:

```bash
python frequent_items_stream.py <n> <phi> <epsilon> <delta> <d> <w> <portExp>
