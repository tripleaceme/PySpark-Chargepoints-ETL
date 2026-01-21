# Electric Charge Points ETL Pipeline

This project contains a **PySpark-based ETL pipeline** to process electric vehicle charge point data from the UK Department for Transport (2017). The pipeline:

* Reads raw CSV data containing charging events
* Computes **maximum and average plugin durations (in hours)** per charge point
* Saves the results in **Parquet format** for easy consumption

This pipeline ensures one row per charge point with clean, rounded results.

---

## Project Structure

```
chargepoints-etl/
│
├── data/
│   ├── input/                  # Place your CSV input file here
│   │   └── electric-chargepoints-2017.csv
│   └── output/                 # ETL output will be saved here in Parquet format
│
├── src/
│   └── etl_job.py              # ETL pipeline implementation
│
├── README.md
└── requirements.txt            # Python dependencies
```

---

## Requirements

* Python 3.12
* PySpark 3.5.5 or above

**Install dependencies:**

```bash
pip install pyspark
```

---

## Usage

1. Place the input CSV file in `data/input/`:

   ```
   data/input/electric-chargepoints-2017.csv
   ```

2. Run the ETL pipeline:

```bash
python src/etl_job.py
```

3. The output Parquet file will be saved in:

```
data/output/chargepoints-2017-analysis
```

---

## ETL Pipeline Details

### 1. Extract

* Reads raw CSV using PySpark
* Infers column types automatically

### 2. Transform

* Groups data by `CPID` (charge point ID)
* Computes:

  * `max_duration` – longest plugin session per charge point
  * `avg_duration` – average plugin session per charge point
* Rounds both values to **2 decimal places** for consistency

### 3. Load

* Saves the aggregated results as a **Parquet file**
* Can be easily read by Spark, Pandas, or other big data tools

---

## Example Output

| chargepoint_id | max_duration | avg_duration |
| -------------- | ------------ | ------------ |
| AN06056        | 11.98        | 4.76         |
| AN07263        | 7.83         | 3.64         |

---

## Contributing

* Fork the repo
* Create a new branch
* Submit PRs with improvements, bug fixes, or optimizations

---

## License

MIT License
