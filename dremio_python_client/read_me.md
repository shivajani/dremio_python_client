# Dremio Python Client Integration Patterns

This document outlines integration patterns for connecting to Dremio using Python clients, including REST API and JDBC, with supported Dremio versions, pros, cons and limitations.

---

## Integration Patterns Overview

| Pattern              | Min Dremio Version | Max Dremio Version | Pros                                                                 | Cons / Limitations                                                                                  |
|----------------------|-------------------|-------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| REST API             | 4.x               | Latest            | Simple HTTP calls, no JDBC driver needed, easy automation            | Limited to API endpoints, not suitable for large data transfers, may lack advanced SQL capabilities |
| JDBC (JayDeBeApi)    | 4.x            | Latest            | Full SQL support, works with pandas/pyarrow, supports large datasets | Requires Java, JDBC driver, more setup, version compatibility issues                                |
| Flight (Arrow Flight)| 18.0.0            | Latest            | High-performance data transfer, native Arrow support, scalable       | Requires Flight server enabled, Python Arrow Flight library, may need extra configuration           |

---

## Dremio Client Configuration

## JDBC Driver Download

 - Download JDBC driver:   
            [dremio-jdbc-driver-18.0.0](https://download.dremio.com/jdbc-driver/18.0.0-202109101536100970-a32fc9f4/dremio-jdbc-driver-18.0.0-202109101536100970-a32fc9f4.jar)

 `Note: copy into ./jdbc-driver folder`

## Python Environment Setup

### Using Conda

```bash
conda create -n dremio_v18_client
conda activate dremio_v18_client
conda install python=3.11 pandas pyarrow jaydebeapi
```

### Using Virtualenv

```bash
python -m venv dremio_v18_client
source dremio_v18_client/bin/activate  # On Windows: dremio_v18_client\Scripts\activate
pip install pandas pyarrow jaydebeapi
```

---



---

## SQL Files and Scenarios

Place your SQL files under the `.src` directory.

```
/project-root
  └── .src/
          ├── example_query.sql
          └── another_query.sql
```

## How to Run

1. **Run Example Script**  
    Execute your Python script (e.g., `dremio_utils_v18.py`):
    ```bash
    conda activate dremio_v18_client
    cd <INSTALL_DIR>
    python dremio_utils_v18.py --sql-file example_query.sql
    ```


> **Note:** Adjust script arguments and environment variables as needed for your setup.