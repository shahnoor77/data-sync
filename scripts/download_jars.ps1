Write-Host "Downloading Debezium JAR files..." -ForegroundColor Green

# Create directories
$libDir = "debezium/lib"
New-Item -ItemType Directory -Force -Path $libDir | Out-Null

# Debezium version
$debeziumVersion = "2.5.0.Final"
$kafkaVersion = "3.6.1"

# Base URLs
$mavenBase = "https://repo1.maven.org/maven2"

# JAR files to download
$jars = @(
    @{
        Name = "debezium-embedded"
        Group = "io/debezium/debezium-embedded"
        Version = $debeziumVersion
    },
    @{
        Name = "debezium-core"
        Group = "io/debezium/debezium-core"
        Version = $debeziumVersion
    },
    @{
        Name = "debezium-connector-postgres"
        Group = "io/debezium/debezium-connector-postgres"
        Version = $debeziumVersion
    },
    @{
        Name = "debezium-connector-mysql"
        Group = "io/debezium/debezium-connector-mysql"
        Version = $debeziumVersion
    },
    @{
        Name = "kafka-connect-api"
        Group = "org/apache/kafka/connect-api"
        Version = $kafkaVersion
    },
    @{
        Name = "kafka-clients"
        Group = "org/apache/kafka/kafka-clients"
        Version = $kafkaVersion
    },
    @{
        Name = "slf4j-api"
        Group = "org/slf4j/slf4j-api"
        Version = "2.0.9"
    },
    @{
        Name = "slf4j-simple"
        Group = "org/slf4j/slf4j-simple"
        Version = "2.0.9"
    }
)

foreach ($jar in $jars) {
    $filename = "$($jar.Name)-$($jar.Version).jar"
    $url = "$mavenBase/$($jar.Group)/$($jar.Version)/$filename"
    $output = "$libDir/$filename"
    
    if (Test-Path $output) {
        Write-Host "  ✓ $filename already exists" -ForegroundColor Yellow
    } else {
        Write-Host "  Downloading $filename..." -ForegroundColor Cyan
        try {
            Invoke-WebRequest -Uri $url -OutFile $output
            Write-Host "  ✓ Downloaded $filename" -ForegroundColor Green
        } catch {
            Write-Host "  ✗ Failed to download $filename" -ForegroundColor Red
            Write-Host "    Error: $_" -ForegroundColor Red
        }
    }
}

Write-Host "`n✓ JAR download complete!" -ForegroundColor Green
