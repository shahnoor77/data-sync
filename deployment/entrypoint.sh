#!/bin/bash
set -e

echo "EMQX 5.x Secure Startup - Loading TLS certificates from environment"

# Create certs directory if it doesn't exist
mkdir -p /opt/emqx/etc/certs

# Check if TLS certificates are provided via environment variables
if [ -n "$EMQX_SSL_CERT_B64" ] && [ -n "$EMQX_SSL_KEY_B64" ] && [ -n "$EMQX_SSL_CA_B64" ]; then
    echo "Loading TLS certificates from environment variables..."
    
    # Decode base64 certificates and save to files
    echo "$EMQX_SSL_CA_B64" | base64 -d > /opt/emqx/etc/certs/ca-cert.pem
    echo "$EMQX_SSL_CERT_B64" | base64 -d > /opt/emqx/etc/certs/server-cert.pem
    echo "$EMQX_SSL_KEY_B64" | base64 -d > /opt/emqx/etc/certs/server-key.pem
    
    # Set proper permissions
    chmod 644 /opt/emqx/etc/certs/ca-cert.pem
    chmod 644 /opt/emqx/etc/certs/server-cert.pem
    chmod 600 /opt/emqx/etc/certs/server-key.pem
    
    echo "TLS certificates loaded successfully"
    echo "  CA Certificate: /opt/emqx/etc/certs/ca-cert.pem"
    echo "  Server Certificate: /opt/emqx/etc/certs/server-cert.pem"
    echo "  Server Private Key: /opt/emqx/etc/certs/server-key.pem"
else
    echo "WARNING: TLS certificates not provided via environment variables"
    echo "Expected environment variables:"
    echo "  EMQX_SSL_CA_B64 - Base64 encoded CA certificate"
    echo "  EMQX_SSL_CERT_B64 - Base64 encoded server certificate"
    echo "  EMQX_SSL_KEY_B64 - Base64 encoded server private key"
    echo ""
    echo "TLS listeners (8883, 8084) may fail to start without certificates"
fi

# Validate configuration before starting
echo "Validating EMQX configuration..."
if ! emqx check_config; then
    echo "ERROR: EMQX configuration validation failed"
    exit 1
fi

echo "Configuration validation passed"
echo "Starting EMQX 5.x in foreground mode..."

# Start EMQX in foreground
exec emqx foreground