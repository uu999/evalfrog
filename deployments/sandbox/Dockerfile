# The tag is intentionally fixed at build time by the deployment pipeline.
# Production deployments must pin the resulting image by digest and use a
# hardened OCI runtime (runsc); runc is retained only for local/CI protocol tests.
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

RUN useradd --system --uid 65532 --create-home sandbox
COPY runner.py /opt/evalfrog/runner.py
RUN chmod 0555 /opt/evalfrog/runner.py

USER 65532:65532
WORKDIR /tmp
ENTRYPOINT ["python", "-I", "-S", "/opt/evalfrog/runner.py"]
