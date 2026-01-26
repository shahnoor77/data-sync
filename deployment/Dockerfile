FROM emqx/emqx:5.4.1

# Set working directory
WORKDIR /opt/emqx

# Create directories with proper ownership (run as root first)
USER root
RUN mkdir -p /opt/emqx/etc/certs \
    && mkdir -p /opt/emqx/data \
    && mkdir -p /opt/emqx/log \
    && chown -R 1000:1000 /opt/emqx/etc/certs \
    && chown -R 1000:1000 /opt/emqx/data \
    && chown -R 1000:1000 /opt/emqx/log

# Copy TLS certificates with proper permissions, do not add these in the production due to security risk, use variables.
COPY --chown=1000:1000 certs/ca-cert.pem /opt/emqx/etc/certs/ca-cert.pem
COPY --chown=1000:1000 certs/server-cert.pem /opt/emqx/etc/certs/server-cert.pem
COPY --chown=1000:1000 certs/server-key.pem /opt/emqx/etc/certs/server-key.pem

# Copy configuration files
COPY --chown=1000:1000 emqx.conf /opt/emqx/etc/emqx.conf
COPY --chown=1000:1000 acl.conf /opt/emqx/etc/acl.conf

# Set proper permissions for certificates (security critical)
RUN chmod 644 /opt/emqx/etc/certs/ca-cert.pem \
    && chmod 644 /opt/emqx/etc/certs/server-cert.pem \
    && chmod 600 /opt/emqx/etc/certs/server-key.pem \
    && chmod 644 /opt/emqx/etc/emqx.conf \
    && chmod 644 /opt/emqx/etc/acl.conf

# Switch to emqx user (UID 1000)
USER 1000

# Environment variables for node configuration (minimal defaults)
ENV EMQX_NODE__NAME=emqx@127.0.0.1
ENV EMQX_CLUSTER__DISCOVERY_STRATEGY=manual

# Default secrets (MUST be overridden in production), do not use directly in production 
ENV EMQX_NODE__COOKIE=emqxsecretcookie
ENV EMQX_DASHBOARD__DEFAULT_USERNAME=
ENV EMQX_DASHBOARD__DEFAULT_PASSWORD=

# Expose ports
EXPOSE 1883 8883 8083 8084 18083 4370

# Create volumes for persistence (stateless by default)
VOLUME ["/opt/emqx/data", "/opt/emqx/log"]

# Health check (fail fast policy)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD emqx_ctl status || exit 1

# Start EMQX with foreground mode (no pre-initialization commands)
CMD ["emqx", "foreground"]