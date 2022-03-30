<div id="top"></div>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/codefo-O/on_prem_etl_pipeline">
    <img src="images/logo.png" alt="Logo" width="300" height="300">
  </a>

<h3 align="center">ETL Pipeline</h3>

  <p align="center">
    A proof of concept project to create an ETL pipeline to ingest data from a CSV/JSON file, transform, saves as parquet and visualize for analysis.
    <br />
    <a href="https://youtu.be/lBjqDYMJ9kA">View Demo Video</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
     <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#Deployment">Deployment</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

<img src="images/diagram.png">

The workflow for the project.

Step 1: User generates file and copies to a folder

Step 2: Monitor folder using inotify-tools for new files

Step 3: Trigger Airflow Dag using inotify-tools

Step 4: Airflow Dag runs Spark job

Step 5: Spark generates parquet files directly to the filesystem

Step 6: Query filesystem using Drill

Step 7: Visualize data using Superset

<p align="right">(<a href="#top">back to top</a>)</p>


### Built With
* [Airflow](https://airflow.apache.org/)
* [Bash](https://www.gnu.org/software/bash/)
* [Docker](https://www.docker.com/)
* [Drill](https://drill.apache.org/)
* [Postgres](https://www.postgresql.org/)
* [PySpark](https://pypi.org/project/pyspark/)
* [Spark](https://spark.apache.org/)
* [Superset](https://superset.apache.org/)

<p align="right">(<a href="#top">back to top</a>)</p>

### Prerequisites

This project can be ran on any server able to run Docker containers and inotify-tools available in EPEL.

* [Docker](https://www.docker.com/)
* [inotify-tools](https://docs.fedoraproject.org/en-US/epel/)

### Deployment

To deploy the etl_pipline solution please follow the steps below.
1. Clone the repo.
   ```sh
   git clone https://github.com/codefo-O/on_prem_etl_pipeline.git
   ```
2. Change into the work directory.
   ```sh
   cd on_prem_etl_pipeline
   ```
3. Start the Postgres container.
   ```sh
   docker run -dit --name postgres \
                           -e POSTGRES_USER=airflow \
                           -e POSTGRES_PASSWORD=airflow \
                           -e POSTGRES_DB=airflow postgres:latest
   ```
4. Start the Apache Airflow & Apache Spark container.
   ```sh
   docker run -dit --name airflow \
                            -v ${PWD}/dags:/usr/local/airflow/dags \
                            -v ${PWD}/data:/data -v ${PWD}/jars:/jars \
                            -v ${PWD}/scripts:/scripts --link postgres:postgres \
                            -p 8080:8080 codefoo/airflow-spark webserver
   ```
5. Add Admin user to Apache Airflow.
   ```sh
   docker exec -it airflow airflow users create \
                                          --role Admin \
                                          --username admin \
                                          --email admin \
                                          --firstname admin \
                                          --lastname admin \
                                          --password admin
   ```
6. Update Apache Airflow database.
   ```sh
   docker exec -it airflow airflow db upgrade
   ```
7. Start Apache Airflow scheduler for the first time.
   ```sh
   docker exec -dit airflow airflow scheduler
   ```
8. Start the Apache Drill container.
   ```sh
   docker run -dit --name drill -v ${PWD}/data:/data -p 8047:8047 apache/drill:latest
   ```
9. Start the Apache Superset container.
   ```sh
   docker run -dit --name superset -p 8088:8088 codefoo/superset-sqlalchemy:latest
   ```
10. Add Admin user to Apache Superset. 
    ```sh
    docker exec -it superset superset fab create-admin \
                                           --username admin \
                                           --firstname Superset \
                                           --lastname Admin \
                                           --email admin@superset.com \
                                           --password admin
    ```
11. Update Apache Superset database.
    ```sh
    docker exec -it superset superset db upgrade
    ```
11. Initalize Superset.
    ```sh
    docker exec -it superset superset init
    ```


<p align="right">(<a href="#top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

Once you have deployed all the containers and confirmed everything is working you can test the pipeline by doing the steps below.
1. Start inotify-tools monitoring script
    ```sh
    cd data
    ./monitor_incoming.sh
    ```
2. Copy files into /data/incoming.
    ```sh
    cp filename incoming/ 
    example: cp 100_Records.csv incoming/
    ```
3. To test a invalid file.
    ```sh
    cp failcheck.txt incoming/ 
    ```

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- LICENSE -->
## License

Distributed under the Apache 2.0 License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Gurjot Singh - GurjotSinghJheeta@Gmail.com

Project Link: [https://github.com/codefo-O/on-prem_etl_pipeline](https://github.com/codefo-O/on-prem_etl_pipeline)

View Demo Video: [https://youtu.be/lBjqDYMJ9kA](https://youtu.be/lBjqDYMJ9kA)
