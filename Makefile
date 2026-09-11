.PHONY: requirements

requirements:
	# pyspark and py4j are excluded: the spark-k8s-glue base image already
	# ships them at /opt/spark/python, matching its own Spark jars. Installing
	# them again from pip is slow and pulls a mismatched version.
	uv export --format requirements-txt --no-dev \
		--no-emit-package pyspark --no-emit-package py4j > requirements.txt
