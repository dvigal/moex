# PyMOEX
Unofficial ISS [MOEX](https://iss.moex.com/iss/reference/) API on Python 
# Installation
Run the following to instal PyMOEX
```
git clone https://github.com/dvigal/moex.git
pip install .
```

# Dependencies
PyMOEX API runs on Python 3. You'll also need pip.

PyMOEX depends on the following Python packages:
> [Pandas](http://pandas.pydata.org/) A powerful data analysis / manipulation library for Python.

# Usage examples
```
from moex import MOEX
moex = MOEX()
data = moex.history_engines_stock_totals_securities(date_start='2018-01-01', date_end='2018-08-16', secid=['SBER'])
data[["SYSTIME", "SECID", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]]
```
![output](https://i.imgur.com/gq1tHfe.png)
```
sber = data
sber = sber.set_index(DatetimeIndex(sber['DATE']))
sber["VOLUME"] = sber["VOLUME"].apply(float) / 1000000
sber["CLOSE"] = sber["CLOSE"].apply(float)
sber["OPEN"] = sber["OPEN"].apply(float)
sber["LOW"] = sber["LOW"].apply(float)
sber_weekly = sber[["DATE", "CLOSE", "OPEN", "HIGH", "LOW", "VOLUME"]].groupby(Grouper(freq='W', level=0)).agg({"CLOSE" : "max", "OPEN" : "min", "HIGH" : "first", "LOW" : "first", "VOLUME" : "mean"})
sber_weekly["CLOSE"].apply(float).plot(figsize=(16,4), title="Weekly", grid=True, legend=True)
sber_weekly["OPEN"].apply(float).plot(figsize=(16,4), title="Weekly", grid=True, legend=True)
sber_weekly["HIGH"].apply(float).plot(figsize=(16,4), title="Weekly", grid=True, legend=True)
sber_weekly["LOW"].apply(float).plot(figsize=(16,4), title="Weekly", grid=True, legend=True)
sber_weekly["VOLUME"].apply(float).plot(figsize=(16,4), title="Weekly", grid=True, legend=True)
```
![output](https://i.imgur.com/JjyqsZh.png)
