# Mark to Future Cube Spark Data Source

[![Build Status](https://travis-ci.org/FRosner/spark-mtf-cube.svg?branch=master)](https://travis-ci.org/FRosner/spark-mtf-cube)

## Description

Spark data source for [Mark to Future](http://www.cfapubs.org/doi/pdf/10.2469/dig.v31.n1.829) cube binary files.
It only supports reading data at the moment, but no writing.
It is recommended to persist cube data in parquet format if required.

## Usage

### Example

```scala
val spark = SparkSession.builder.master("local").getOrCreate
val df = spark.read.format("de.frosner.spark.mtf")
  .option("csrFile", "src/test/resources/withxml/cube.csr")
  .load("src/test/resources/withxml/cube.dat.0")
df.show()
```

```
+--------------+------------+--------------------+--------+-------------+-----+
|          Time|BaseCurrency|          Instrument|Currency|     Scenario|Value|
+--------------+------------+--------------------+--------+-------------+-----+
|2000/01/01 (0)|         EUR|[Instrument 1,Typ...|    null|Base Scenario|  0.0|
|2000/01/01 (0)|         EUR|[Instrument 1,Typ...|    null|         MC_1|  0.0|
|2000/01/01 (0)|         EUR|[Instrument 1,Typ...|    null|         MC_2|  0.0|
|2000/01/01 (0)|         EUR|[Instrument 2,Typ...|    null|Base Scenario|  0.0|
|2000/01/01 (0)|         EUR|[Instrument 2,Typ...|    null|         MC_1|  0.0|
|2000/01/01 (0)|         EUR|[Instrument 2,Typ...|    null|         MC_2|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR1|Base Scenario|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR1|         MC_1|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR1|         MC_2|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR2|Base Scenario|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR2|         MC_1|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR2|         MC_2|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR3|Base Scenario|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR3|         MC_1|  0.0|
|2000/01/01 (0)|         EUR|                null|    CUR3|         MC_2|  0.0|
+--------------+------------+--------------------+--------+-------------+-----+
```

### Options

#### Base Options

Option | Description | Possible Values | Default
--- | --- | --- | ---
`csrFile` | Path to the meta data XML file. The library will try to load it first from the driver and then from the executors. | local or cluster path | -
`checkCube` | Verify that the cube has the correct size | {`true`, `false`} | `false`

#### Additional Options

Option | Description | Possible Values | Default
--- | --- | --- | ---
`numTimes`* | Number of time points simulated | int > 1 |-
`numInstruments`* | Number of instruments simulated | int > 1 | -
`numScenarios`* | Number of scenarios simulated | int > 1 | -
`endianType`* | Byte ordering in the data files | {`LittleEndian`, `BigEndian`} | -
`valueType`* | Value type of the simulated values | {`FloatType`, `DoubleType`} | -

\* only used and required if `csrFile` is not specified

### Schema

```
root
 |-- Time: string (nullable = false)
 |-- BaseCurrency: string (nullable = false)
 |-- Instrument: struct (nullable = true)
 |    |-- ID: string (nullable = false)
 |    |-- InstrType: string (nullable = false)
 |    |-- Unit: string (nullable = false)
 |-- Currency: string (nullable = true)
 |-- Scenario: string (nullable = false)
 |-- Value: float (nullable = false)
```

Column | Description
--- | ---
Time | Simulation time dimension of the cube
BaseCurrency | Base currency of the simulation
Instrument | Simulated instrument dimension if the simulatable dimension is an instrument, else null
Currency | Simulated currency dimension if the simulatable dimension is a currency, else null
Scenario | Scenario dimension of the cube
Value | Simulated value

## Data Source Format

Cube files consist of a meta data file (XML) and a data file (binary encoded sequence of numerics).

### Meta Data

The meta data is read from an XML file. This file contains information about the different cube dimensions plus
additional meta information about the simulation run.

#### File Structure

Currently the following entries are parsed from the XML tree:

Node | Property
--- | ---
`/resultInfo/prop(name=precision)` | value type
`/resultInfo/prop(name=endianType)` | byte ordering of values
`/timeDimensionInfo/timeList/timePoint` | time dimensions
`/simulatableDimensionInfo/SADescriptor` | instrument dimensions
`/scenarioDimensionInfo/scenarioList/scenarioInfo` | scenario dimensions

#### Example

An [example file](src/test/resources/withxml/cube.csr) can be found in the test resources.

### Data

#### File Structure

The data file is in binary format and encoded as a sequence of numerical values.
It corresponds to a three dimensional cube (time _t_, instrument _i_, scenario _s_) containing values _x_ which can be either float or double.
The file consists of one binary record per time.
Each time record consists of individual instrument records.
Each instrument record contains the values for each scenario.

Given three instruments, two scenarios and two times, the structure looks as follows:

![x_t1i1s1 x_t1i1s2 x_t1i2s1 ... x_t2i3s2](http://mathurl.com/jmnj95m.png)

#### Command Line Inspection

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
