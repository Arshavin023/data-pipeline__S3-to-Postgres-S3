# data-pipeline__S3-to-Postgres-S3

### STEPS:
Clone the repository
```bash
https://github.com/Arshavin023/data-pipeline__S3-to-Postgres-to-S3
```

### Step
1. Download docker-desktop and install, ensuring WSL is appropriately installed
2. Launch Docker-Desktop and login with credentials
3. create Dockerfile file
4. create docker-compose.yml file
5. right-click on Dockerfile file and select 'build image'
6. if step 5 is successful, an airflow folder appears. Navigate to it and create a folder named 'dags'
7. Inside the dags folder, update the dag according to your requirement
8. if step 5 is successful, right click docker-compose.yml file and select 'compose up'
9. Open Docker-Desktop, confirm is images are running. If yes, navigate to containers and access the airflow UI through the provided URL with port: 8080:8080
10. To login to Airflow UI, obtain password from the 'standalone_admin_password.txt' file in airflow folder
11. Login to Airflow UI, click on Admin at the top center, select Variables and create required Variables . click on Admin again
Note: when running PostgreSQL through Docker, the host should be 'host.docker.internal' and not 'localhost'
12. Now click on the dag and trigger it manually.








