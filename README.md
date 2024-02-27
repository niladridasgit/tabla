# tabla
This project is designed to extract table names from HQL and SQL files referenced within Python scripts defining Airflow DAGs. It aids individuals seeking to retrieve table names mentioned in one or more SQL queries, simplifying the process of gathering metadata from multiple files efficiently.

###### required change(s) in the main script named `Tables inside HQL Queries mentioned in Python Scripts.py`
| line number | required change |
|----------|----------|
| 5 | change `working_directory` variable with the path where these files are located in your local system |

The main script retrieves the table names from HQL/SQL files located in the `queries` directory which are mentioned in Python scripts defining Airflow DAGs located in the `dags` directory, and then the retrived table names are saved into a CSV file named `final.csv`.
