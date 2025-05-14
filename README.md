# LSH analyzer using Docker Cluster Setup

- This project evaluates whether LSH (Locality-Sensitive Hashing) is a suitable model for selecting similar questions.
- The analysis is based on files containing:
    - Questions.
    - Pairs of similar and non-similar questions.

## Setup

- Based on the tutorial: [Setting up a Spark Standalone Cluster on Docker](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b).

### Build

1. Build and start the cluster:
     ```bash
     docker compose build
     docker-compose up --scale spark-worker=6
     ```
     - You can adjust the number of workers as needed.

2. Stop the cluster:
     ```bash
     docker compose down
     ```

3. Run the analyzer:
     ```bash
     docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/lsh.py
     ```

### Description of Docker Setup

- **Dockerfile**:
    - Includes a standard setup.
    - Creates directories and sets environment variables.
    - Downloads Spark and Python requirements (including `pyspark`).
    - Configures Spark using `spark-defaults.conf` with:
        - Standard configurations.
        - Custom settings (e.g., increased memory limits for executors).
    - Copies the `entrypoint.sh` script.

- **entrypoint.sh**:
    - A shell script that starts cluster instances as described in the `docker-compose.yml` file.

- **docker-compose.yml**:
    - Defines three main services:
        1. **spark-master**:
             - Acts as the cluster manager.
             - Coordinates tasks and resources.
             - Exposes:
                 - Port `7077` for worker communication.
                 - Port `8080` (mapped to `9090`) for the web UI.
        2. **spark-worker**:
             - Executes tasks assigned by the master.
             - Configured with specific resource limits (e.g., memory and CPU).
             - Connects to the master.
        3. **spark-history-server**:
             - Provides a web interface (on port `18080`) to view completed Spark jobs.
             - Uses the shared `spark-logs` volume to access event logs.

## LSH analysis

### **Python File for Execution**

The **Python file** responsible for execution is located at:

```git 
spark_apps/lsh.py
```

### Analysis Results

Below is a table summarizing the accuracy data for various parameter configurations. The values represent the mean of multiple observations. Detailed results for each experiment can be found in the file `book_data/result.txt`.

| Shingle Size | Signature Size | Number of Bands | True Positives (TP) | False Positives (FP) |
|--------------|----------------|------------------|----------------------|----------------------|
| 2            | 100            | 20               | 93,631              | 86,968              |
| 3            | 50             | 5                | 14,274              | 12,227              |
| 3            | 100            | 10               | 19,179              | 16,578              |
| 3            | 100            | 20               | 62,447              | 53,197              |
| 3            | 200            | 40               | 76,441              | 65,687              |
| 3            | 200            | 100              | 147,050             | 210,637             |
| 4            | 50             | 10               | 36,463              | 32,977              |
| 4            | 100            | 10               | 13,671              | 13,618              |
| 4            | 100            | 20               | 48,910              | 43,113              |
| 4            | 100            | 25               | 70,293              | 61,260              |
| 5            | 100            | 20               | 39,617              | 36,643              |

For most observations, the precision (calculated as TP / (TP + FP)) is approximately 0.54.

### Observations

- **Shingle Size**: The best precision is achieved with shingle sizes of 3 or 4. A size of 2 appears to be too small, while larger sizes may lead to reduced precision.
- **Signature Size**: Optimal values for the signature size are around 100.
- **Number of Bands**: The best results are observed with 20–25 bands.

### Conclusion

While LSH shows some promise, it does not appear to be the most effective model for predicting question similarity. In most experiments, the precision achieved is only slightly above 0.5, indicating limited reliability for this use case.

