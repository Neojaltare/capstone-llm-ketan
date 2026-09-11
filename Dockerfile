# The base image is published for linux/amd64 only, so on an Apple Silicon
# machine this builds and runs under emulation. Conveyor runs amd64 anyway,
# so pinning the platform also keeps local and deployed images identical.
FROM --platform=linux/amd64 public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3

# The base image ships PySpark at /opt/spark/python but leaves it off the
# import path. Pointing PYTHONPATH at it means we do not pip install PySpark
# again -- which is slow under emulation, and would install a 4.1.1 client
# against this image's Spark 4.0.1 jars.
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

WORKDIR /opt/spark/work-dir

# Dependencies first, so this layer is cached and only rebuilds when
# requirements.txt changes. Editing src/ then skips the reinstall entirely.
#
# The "-e ." line is stripped: uv exports it to install the project itself, but
# at this point src/ has not been copied yet. It also carries no hash, and pip
# refuses a mixed file once any entry has one (168 of them do here).
COPY requirements.txt ./
RUN grep -v '^-e \.$' requirements.txt > /tmp/dependencies.txt \
    && pip install --no-cache-dir -r /tmp/dependencies.txt \
    && rm /tmp/dependencies.txt

# Now the project itself. --no-deps because everything it needs is installed
# above, pinned and hash-checked.
# README.md is copied because pyproject.toml declares `readme = "README.md"`,
# and hatchling refuses to generate metadata if the file is missing.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

# Overrides the base image's spark-submit entrypoint so DockerOperator can pass
# plain arguments, e.g. command="--tag airflow".
ENTRYPOINT ["python3", "-m", "capstonellm.tasks.clean"]
