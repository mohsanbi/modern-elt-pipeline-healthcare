FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.0

WORKDIR /usr/app
COPY . .
 
#install below two dependencies if we have special condition otherwise no need as in our case we don't need them
# Install Python packages for dbt if you have any hooks or custom scripts
# RUN pip install -r requirements.txt