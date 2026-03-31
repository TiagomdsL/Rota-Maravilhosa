# Rota-Maravilhosa
A Cloud Native App for data analisys of US accidents between 2016 and 2023

To run our application it is needed to download the data set and save it in the "dataset" folder.
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

Our dataset loading is limited to half million samples due to performance issues. We will address this problem later.

In order to run our program it is mandatory to have Docker Desktop installed. The command to run is docker-compose up --build.
To access the endpoints we used Swagger with the following URL http://127.0.0.1:8000/docs#/.

