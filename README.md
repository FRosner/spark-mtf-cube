# Mark to Future Cube Spark Data Source

[![Build Status](https://travis-ci.org/FRosner/spark-mtf-cube.svg?branch=master)](https://travis-ci.org/FRosner/spark-mtf-cube)

## Description

Spark data source for [Mark to Future](http://www.cfapubs.org/doi/pdf/10.2469/dig.v31.n1.829) cube binary files.
It only supports reading, but not writing, data at the moment.
It is recommended to persist cube data in parquet format if you are writing it back.

## Usage

### Example

```scala
val df = spark.read.format("de.frosner.spark.mtf")
    .option("numTimes", "1")
    .option("numInstruments", "1")
    .option("numScenarios", "1")
    .option("endianType", "LittleEndian")
    .option("valueType", "FloatType")
    .load("src/test/resources/small")
df.show()
```

### Options

Option | Description | Possible Values | Default
--- | --- | ---
`checkCube` | Verify that the cube has the correct size | {`true`, `false`} | `false`
`numTimes` | Number of time points simulated* | int > 1 |-
`numInstruments` | Number of instruments simulated* | int > 1 | -
`numScenarios` | Number of scenarios simulated* | int > 1 | -
`endianType` | Byte ordering in the data files* | {`LittleEndian`, `BigEndian`} | -
`valueType` | Value type of the simulated values* | {`FloatType`, `DoubleType`} | -

* only required if the `cube.csr` meta data file is not available

### Schema

Column | Description
--- | ---
Time | Simulation time dimension of the cube
Instrument | Instrument dimension of the cube
Scenario | Scenario dimension of the cube
Value | Simulated value

## Data Source Format

Cube files consist of a meta data file (XML) and a data file (binary encoded sequence of numerics).
All files are expected to be located in the same folder without any subdirectories.
The meta data file needs to be called `cube.csr`, while the data files are called `cube.dat.*`.

### Meta Data

[TODO]

### Data

#### File Structure

The data file is in binary format and encoded as a sequence of numerical values.
It corresponds to a three dimensional cube (time _t_, instrument _i_, scenario _s_) containing values _x_ which can be either float or double.
The file consists of one binary record per time.
Each time record consists of individual instrument records.
Each instrument record contains the values for each scenario.

Given three instruments, two scenarios and two times, the structure looks as follows:

![x_t1i1s1 x_t1i1s2 x_t1i2s1 ... x_t2i3s2](http://mathurl.com/jmnj95m.png)

#### Example

```sh
od -f cube.dat.0 | head
```

```
0000000        34234.83        53654.33        34234.45        66456.46
0000020       21312.945        53453.83        23233.32        66456.74
0000040       88888.945        11332.65        55552.22        31231.61
0000060        32984.69        76532.59        65400.29        43259.08
0000080        34234.83        53654.33        34234.45        66456.46
0000100       21312.945        53453.83        23233.32        66456.74
0000120       88888.945        11332.65        55552.22        31231.61
0000140        32984.69        76532.59        65400.29        43259.08
```