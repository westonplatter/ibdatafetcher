# IB Data Fetcher
Simple methods for getting market data from IB

## Running
```
make env.create
make env.update
```

See configs and code in `ibdatafetcher/client.py`

```
conda activate ibdatafetcher
python ibdatafetcher/client.py
```

<!--
## Using with QuantDataLake
[QuantDataLake](https://github.com/westonplatter/QuantDataLake) is a helpful tool for creating & maintaining data lakes with quant data. The goal is to make it easy to create the AWS infrastructure for storing flexible data sets and programtically retrieving them.

In the future, `ibdatafetcher` will upload data to the data lake for future use.
-->

## License
See LICENSE file.
