<div align="center">

# DaseX: A Vectorized And Pipelined Query Execution Engine

</div>

## Table of Contents

- [Vision & Scope](#vision--scope)
- [Dependencies & Environment](#dependencies--environment)
- [Build Instructions](#build-instructions)
- [Running & Testing](#running--testing)
- [TPC-H Data & Queries](#tpc-h-data--queries)
- [Logging & Profiling](#logging--profiling)
## Vision & Scope

DaseX explores:

1. Vectorized batch processing (operator fusion within a batch, cache-friendly access)
2. Pipeline-driven physical execution (reducing intermediate materialization)
3. Multithreaded parallelism (fixed thread pool + task scheduling)
4. Columnar in-memory layout (Apache Arrow format for uniform column handling / SIMD)
5. Unified expression & scalar function framework (extensible UDF support)
6. Experimental operators & expressions: LIKE / CASE / IN / Aggregation / Joins (Hash, etc.) / ORDER / LLM placeholder

## Dependencies & Environment

Minimum toolchain:

- C++17
- GCC / G++ 10.3.1 (or compatible)
- CMake ≥ 3.22

Runtime / feature dependencies:

- jemalloc
- libnuma
- pthread
- Apache Arrow (+ Parquet)
- leveldb
- GoogleTest
- spdlog / fmt / libpg_query

Optional: valgrind (memory leak diagnostics)

### Installation Snippets 
jemalloc:
```
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc-5.3.0
./autogen.sh && make -j8 && sudo make install
```

libnuma:
```
sudo dnf install numactl-devel    # or yum / apt equivalent
```

Apache Arrow (minimal features example):
```
git clone https://github.com/apache/arrow.git
cd arrow/cpp
mkdir build && cd build
cmake -DARROW_COMPUTE=ON -DARROW_PARQUET=ON -DARROW_WITH_ZLIB=ON \
	  -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_CSV=ON \
	  -DCMAKE_BUILD_TYPE=Release ..
make -j16
sudo make install
```

GoogleTest (if manual install needed):
```
tar -zxvf googletest-release-1.12.0.tar.gz
cd googletest-release-1.12.0 && mkdir build && cd build
cmake .. && make -j16 && sudo make install
```

leveldb:
```
git clone --recurse-submodules https://github.com/google/leveldb.git
cd leveldb && mkdir build && cd build
cmake -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j
sudo make install
```

> Note: Several dependencies (gtest, spdlog, fmt, libpg_query) are auto-fetched by `FetchContent` in the top-level `CMakeLists.txt` unless restricted by network policy.

## Build Instructions

```bash
git clone https://github.com/Riand314/DaseX.git
cd DaseX
mkdir build && cd build
cmake ..
make -j$(nproc)
```

Check shared library dependencies:
```bash
ldd ./DaseX_Test
```
Add missing paths:
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/lib
```

## Running & Testing

Run all tests:
```bash
./DaseX_Test
```

Filter (single-thread TPC-H Q1):
```bash
./DaseX_Test --gtest_filter="TPCHTest.Q1SingleThreadTest"
```

4-thread example:
```bash
./DaseX_Test --gtest_filter="TPCHTest.Q1FourThreadTest"
```

Generic custom test:
```bash
./DaseX_Test --gtest_filter="Test.MyTest1"
```

## TPC-H Data & Queries

- Small (≈0.1G) sample data bundled under `test_data/`.
- Multi-thread subset under `test_data/4thread/`.
- 22 standard queries: see files with patterns `tpch_qX.cpp`, `test_tpch_qX.cpp`, or `tpch_by_sqls.cpp`.
- Data directory paths are centralized in `tests/file_dir_config.hpp` (adjust to absolute paths as needed).

## Logging & Profiling

Using `spdlog` timing pattern:
```cpp
auto start = std::chrono::steady_clock::now();
// ... execution logic ...
auto end = std::chrono::steady_clock::now();
spdlog::info("[{}:{}] build elapsed: {}", __FILE__, __LINE__, Util::time_difference(start, end));
```

jemalloc runtime stats:
```bash
MALLOC_CONF=stats_print:true ./DaseX_Test --gtest_filter="TPCHTest.Q1SingleThreadTest"
```

Leak checking:
```bash
valgrind --tool=memcheck --leak-check=full ./DaseX_Test --gtest_filter="TPCHTest.Q1SingleThreadTest"
```
