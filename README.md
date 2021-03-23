# IB Data Fetcher
Simple methods for getting market data from IB

## Running
Create a conda env and install python dependencies,

```
make env.create
make env.update
```

Run IB Gateway on Port `4001`.


```
make download.futures_equities
make download.stocks
```

## License
See LICENSE file.
