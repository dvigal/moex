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
moex = MOEX()
data = moex.history_engines_stock_totals_securities(date_start='2018-01-01', date_end='2018-08-16', secid=['SBER'])
```
