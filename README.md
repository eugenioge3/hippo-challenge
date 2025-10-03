# Pharma Analytics Pipeline

This project provides a data processing pipeline to analyze pharmacy claims data. It ingests pharmacy, claims, and reverts event data, calculates key performance metrics, and generates business recommendations.

The pipeline is built using Python and the Pandas library, chosen for its efficiency and ease of setup on datasets of this scale.

## Project Goals

The application performs the following tasks:
1.  **Reads Data**: Ingests pharmacy, claims, and reverts data from directories of JSON and CSV files.
2.  **Calculates Core Metrics**: Computes claim counts, revert counts, average unit price, and total price, grouped by pharmacy (`npi`) and drug (`ndc`).
3.  **Recommends Top Chains**: Identifies the top 2 cheapest pharmacy chains for each drug based on average unit price.
4.  **Finds Common Quantities**: Determines the top 5 most frequently prescribed quantities for each drug.

## Setup and Execution Guide

Follow these steps to set up the environment and run the analytics pipeline.

### 1. Project Structure

Ensure your project files are organized as follows:
```bash
hippo-challenge/
├── data/
│ ├── claims/
│ ├── pharmacy/
│ └── reverts/
├── output/
├── src/
│ └── main.py
├── requirements.txt
└── README.md
```
### 2. Create and Activate a Virtual Environment

Using a virtual environment is highly recommended to manage dependencies.

```bash
# Create the virtual environment
python3 -m venv venv

# Activate the environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

### 3. Install Dependencies

Install the required Python library from the requirements.txt file.
```bash
pip install -r requirements.txt
```

### 4. Run the Pipeline

Execute the main script from the root directory of the project. The script accepts command-line arguments to specify the locations of the data and the output directory.
```bash
python src/main.py --pharmacy_dirs ./data/pharmacy --claims_dirs ./data/claims --reverts_dirs ./data/reverts --output_dir ./output
```


### 5. Check the output

The ./output/ directory will contain three JSON files with the results of the analysis:
 - goal_2_metrics.json
 - goal_3_recommendations.json
 - goal_4_common_quantities.json



# Notes

#### *Architectural Note: Scaling with PySpark

This implementation uses the Pandas library to fulfill all requirements. This choice was made to ensure the application is lightweight and runs without complex external dependencies like Java. For the provided dataset size, Pandas is highly efficient and more than sufficient.
For a production environment handling significantly larger data volumes (millions or billions of records), a distributed processing framework like Apache Spark would be the architecturally superior choice. The logic for Goals 3 and 4 can be expressed in PySpark to leverage multi-core or multi-node cluster processing.
Below are the PySpark implementations of the functions for Goals 3 and 4, demonstrating how this pipeline is designed to scale. To run this code, pyspark would be added to requirements.txt and a compatible Java JDK (17+) would need to be installed in the execution environment.
Scalable PySpark Im

#### *Data Schema Notes

The initial instructions specified a `pharmacy` schema with an `id` column. However, the provided sample data uses an `npi` column as the unique identifier for pharmacies.
This pipeline has been designed to conform to the **actual data schema** provided. Therefore, it uses the `npi` column as the primary key for pharmacy data without renaming or altering the source columns.