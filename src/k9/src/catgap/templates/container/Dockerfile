FROM python:{{ python_version }}-slim

WORKDIR /app
COPY requirements.txt /app
COPY requirements-beam.txt /app

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV DEBCONF_NOWARNINGS=yes

# Python dependencies
RUN python3 -m pip install --require-hashes --no-deps --use-pep517 --no-cache-dir -r requirements-beam.txt && \
    python3 -m pip install --require-hashes --no-deps --use-pep517 --no-cache-dir -r requirements.txt

# Copy the Apache Beam worker dependencies from the Apache Beam SDK for Python image.
COPY --from=apache/beam_python{{ python_version }}_sdk:{{ beam_sdk_version }} /opt/apache/beam /opt/apache/beam
ENV PATH=$PATH:/opt/apache/beam

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
