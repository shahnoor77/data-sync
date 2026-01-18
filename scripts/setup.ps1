
Write-Host "Setting up Sensor Sync System..." -ForegroundColor Green

# Check if Docker is running
Write-Host "`nChecking Docker..." -ForegroundColor Cyan
try {
    docker info | Out-Null
    Write-Host "✓ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}

# Check if Poetry is installed
Write-Host "`nChecking Poetry..." -ForegroundColor Cyan
try {
    poetry --version | Out-Null
    Write-Host "✓ Poetry is installed" -ForegroundColor Green
} catch {
    Write-Host "✗ Poetry is not installed. Installing..." -ForegroundColor Yellow
    (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
    Write-Host "✓ Poetry installed" -ForegroundColor Green
}

# Create required directories
Write-Host "`nCreating directories..." -ForegroundColor Cyan
$directories = @(
    "data/offsets",
    "data/history",
    "logs",
    "certs",
    "debezium/lib"
)

foreach ($dir in $directories) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Write-Host "  ✓ Created $dir" -ForegroundColor Green
}

# Download Debezium JARs
Write-Host "`nDownloading Debezium JARs..." -ForegroundColor Cyan
& .\scripts\download_jars.ps1

# Install Python dependencies
Write-Host "`nInstalling Python dependencies..." -ForegroundColor Cyan
poetry install

# Create .env if it doesn't exist
if (!(Test-Path ".env")) {
    Write-Host "`nCreating .env file..." -ForegroundColor Cyan
    Copy-Item ".env.example" ".env" -ErrorAction SilentlyContinue
    Write-Host "✓ Created .env file (please update with your credentials)" -ForegroundColor Yellow
}

# Start Docker containers
Write-Host "`nStarting Docker containers..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "`nWaiting for databases to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 20

# Check container health
Write-Host "`nChecking container health..." -ForegroundColor Cyan
docker-compose ps

Write-Host "`n✓ Setup complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. Update .env with your configuration"
Write-Host "  2. Run: poetry run python -m src.sensor_sync.main"
Write-Host "  3. Monitor logs in the logs/ directory"
Write-Host ""