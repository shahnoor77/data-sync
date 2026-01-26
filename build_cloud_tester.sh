#!/bin/bash

echo "🚀 Building EMQX Cloud Stress Tester"


# Check if CA certificate exists
if [ ! -f "emqx-ca-cert.pem" ]; then
    echo "⚠️  WARNING: emqx-ca-cert.pem not found!"
    echo "Please download the CA certificate from your EMQX Cloud dashboard"
    echo "and place it in the project root as 'emqx-ca-cert.pem'"
    echo ""
    echo "For now, creating a placeholder file..."
    echo "You MUST replace this with the actual certificate before testing!"
fi

# Build the Docker image
echo "🔨 Building Docker image..."
docker build -f Dockerfile.live -t stress-tester-cloud .

if [ $? -eq 0 ]; then
    echo "✅ Docker image built successfully!"
    echo ""
    echo "🧪 To test connectivity:"
    echo "docker run --rm stress-tester-cloud python test_emqx_cloud_connection.py"
    echo ""
    echo "🚀 To run stress test:"
    echo "docker run --rm stress-tester-cloud"
    echo ""
    echo "📊 To run with custom parameters:"
    echo "docker run --rm -e TOTAL_MESSAGES=100000 -e CONCURRENCY=2 stress-tester-cloud"
else
    echo "❌ Docker build failed!"
    exit 1
fi