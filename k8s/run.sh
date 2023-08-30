# -------------- Install Helm -------------- #

## MacOS
brew install helm

## Ubuntu
# curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
# sudo apt-get install apt-transport-https --yes
# echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
# sudo apt-get update
# sudo apt-get install helm

## Windows
# choco install kubernetes-helm

# -------------- Apache Airflow -------------- #

## Storage for DAGs & Logs
mkdir ~/Downloads/airflow
mkdir ~/Downloads/airflow/dags
mkdir ~/Downloads/airflow/logs
kubectl delete pv airflow-local-dags-pv
kubectl delete pv airflow-local-logs-pv
kubectl create namespace airflow
kubectl apply -f storage.yaml

## Helm Repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

## Helm Install
kubectl create secret generic airflow-webserver-secret-key --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')" --namespace airflow
helm upgrade --install airflow apache-airflow/airflow -f values.yaml --namespace airflow --version 1.10.0

# minikube service airflow-webserver -n airflow