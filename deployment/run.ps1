# EMQX 5.x Production Deployment Script
# Loads configuration from tunables.env

Write-Host "EMQX 5.x Production Deployment" -ForegroundColor Green

# Validate prerequisites
if (!(Test-Path "tunables.env")) {
    Write-Host "ERROR: tunables.env not found" -ForegroundColor Red
    exit 1
}

if (!(Test-Path "certs/server-key.pem")) {
    Write-Host "ERROR: TLS certificates not found in certs/" -ForegroundColor Red
    exit 1
}

# Build and deploy
Write-Host "Building EMQX production image..." -ForegroundColor Yellow
docker build -t emqx-production:latest . --no-cache

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker build failed" -ForegroundColor Red
    exit 1
}

# Create volumes
docker volume create emqx-data 2>$null
docker volume create emqx-logs 2>$null

# Clean up existing
docker stop emqx-production 2>$null
docker rm emqx-production 2>$null

# Deploy
docker run -d `
    --name emqx-production `
    --restart unless-stopped `
    --env-file tunables.env `
    --health-cmd "emqx_ctl status" `
    --health-interval 30s `
    -p 1883:1883 `
    -p 8883:8883 `
    -p 8083:8083 `
    -p 8084:8084 `
    -p 18083:18083 `
    -v emqx-data:/opt/emqx/data `
    -v emqx-logs:/opt/emqx/log `
    emqx-production:latest

Write-Host "Deployment complete" -ForegroundColor Green
Write-Host "Dashboard: http://localhost:18083" -ForegroundColor Cyan