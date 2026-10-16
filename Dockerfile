FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Airflow providers + test deps, pinned against Airflow's official constraints for this Airflow/Python pair.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt \
      --constraint "${CONSTRAINTS_URL}"

# dbt in its own venv (same Python 3.12) so its deps never clash with Airflow's constraints.
USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv
USER airflow
RUN /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.4" "dbt-bigquery==1.9.1"

# Fail the build early if the runtime is not what the pipeline was designed for.
RUN python -c "import sys; v=sys.version_info; print('python', sys.version); assert (v.major, v.minor) == (3, 12), v" \
 && airflow version \
 && /opt/dbt-venv/bin/dbt --version
