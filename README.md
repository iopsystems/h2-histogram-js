The H2Histogram provides a histogram that is conceptually similar to
[HdrHistogram](http://hdrhistogram.org) but with base-2 buckets, which makes it
noticeably faster. This introduces small modifications to the configurable
options as well.

This module is a pure Javascript implementation of the algorithm, which is
described at [h2histogram.org](https://h2histogram.org).

H2Encoding encodes values from the integer range [0, 2^n) into base-2 logarithmic
bins with a controllable relative error bound.

The number of bins must be less than 2^32, and the largest encodable value must
be less than 2^53.

The histogram is designed to encode integer values only.
