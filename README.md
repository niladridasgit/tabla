# tabla
This project is designed to extract table name(s) from HQL and SQL file(s) referenced within Python script(s) defining Airflow DAG(s). It aids individuals seeking to retrieve table name(s) mentioned in one or more SQL querie(s), simplifying the process of gathering metadata from multiple file(s) efficiently.

###### required change(s) in the main script named `Tables inside HQL Queries mentioned in Python Scripts.py`
| line number | required change |
|----------|----------|
| 5 | change the `working_directory` variable with the path where these files are located in your local system |

### brief
The paths <b>[don't mention the `dags` directory in the path]</b> of Python script(s) <b>[from where you want to retrive table name(s)]</b> located inside the `dags` directory are listed in the file named `dags.txt`.

The main script retrieves the table name(s) from HQL/SQL file(s) located in the `queries` directory which are mentioned in Python script(s) defining Airflow DAG(s) located in the `dags` directory, and then the retrived table name(s) are saved into the CSV file named `final.csv`.
