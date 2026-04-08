Project files:

- `data-extract-script.py` extracts a smaller 100 MB sample from the full NASDAQ ITCH archive.
- `parser-new.py` parses the sample with PySpark and exports `data/dashboard_summary.csv` and `data/stock_summary.csv`.
- `dashboard.py` opens the Streamlit dashboard for visual presentation.

Run order for the demo:

1. Run `data-extract-script.py` once to create `data/sample_itch_100mb.bin`.
2. Run `parser-new.py` to generate the dashboard-ready CSV files.
3. Launch Streamlit with `streamlit run dashboard.py`.

This setup is intended for a single laptop. It uses Spark for the batch parse step, then keeps the dashboard lightweight by reading compact summary CSV files.

python version - 3.10.11
pyspark version - 3.3.2
set PYSPARK_DRIVER_PYTHON=python

set HADOOP_HOME=C:\hadoop
set hadoop.home.dir=C:\hadoop
set PATH=%PATH%;C:\hadoop\bin


docker
docker run -it --rm -v "%cd%:/app" spark-hft-env /bin/bash
/opt/spark/bin/spark-submit parser-new.py