# Deployment Instructions 
## Rota Maravilhosa – Road Risk Intelligence API

---
# 1. deployment/scripts

Create the cluster to support the application 
```bash
./create-cluster.sh
```
Our application uses BigQuery, a cloud service that needs a secret to be accessed. The following commands provides that secret.
```bash
kubectl create secret generic bq-secret \
    --from-literal "API_TOKEN=$(cat ~/bq-key.json)" \
    -n rota-maravilhosa
```
Build and deploy the application. The script uses cloudbuild and applies all the deployment and service files. It also executes hpa and ingress, in order to obtain an IP address to make the application available in the network.
```bash
./build-and-deploy.sh
```

# 2. API access

To acces the API via Swagger UI we need to get the IP ordered by ingress. Execute the following command and wait between 2 to 5 minutes.

```bash
kubectl get ingress -n rota-maravilhosa
```

When the IP is visible in address, access the following URL.
```bash
http://<IP-ADDRESS>/docs
```


