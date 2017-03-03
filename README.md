# Mark to Future Cube Spark Data Source

[![Build Status](https://travis-ci.org/FRosner/spark-mtf-cube.svg?branch=master)](https://travis-ci.org/FRosner/spark-mtf-cube)

## Description

Spark data source for [Mark to Future](http://www.cfapubs.org/doi/pdf/10.2469/dig.v31.n1.829) cube binary files.

## Usage



## File Format

Cube files consist of a meta data file (XML) and a data file (binary encoded sequence of numerics).

### Meta Data



### Data

The data file is in binary format and encoded as a sequence of numerical values.
It corresponds to a three dimensional cube (time _t_, instrument _i_, scenario _s_) containing values _x_ which can be either float or double.
The file consists of one binary record per time.
Each time record consists of individual instrument records.
Each instrument record contains the values for each scenario.

Given three instruments, two scenarios and two times, the structure looks as follows:

![x_t1i1s1 x_t1i1s2 x_t1i2s1 ... x_t2i3s2](http://mathurl.com/jmnj95m.png)