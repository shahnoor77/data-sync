# EMQX TLS Certificate Generation Script for Windows PowerShell
# Generates CA, server, and client certificates for production EMQX deployment

param(
    [string]$CertDir = "certs",
    [int]$Days = 3650,
    [int]$KeySize = 4096
)

# Configuration
$Country = "US"
$State = "California"
$City = "San Francisco"
$Org = "Industrial IoT Corp"
$OU = "MQTT Infrastructure"
$Email = "admin@industrial-iot.com"

# Server configuration
$ServerCN = "emqx-server"
$ServerAltNames = "DNS:localhost,DNS:emqx,DNS:emqx-server,DNS:*.emqx.local,IP:127.0.0.1,IP:0.0.0.0"

Write-Host "🔐 Generating TLS certificates for EMQX..." -ForegroundColor Blue

# Check if OpenSSL is available
try {
    $null = Get-Command openssl -ErrorAction Stop
    Write-Host "✅ OpenSSL found" -ForegroundColor Green
} catch {
    Write-Host "❌ OpenSSL not found. Please install OpenSSL or use Git Bash." -ForegroundColor Red
    Write-Host "   Download from: https://slproweb.com/products/Win32OpenSSL.html" -ForegroundColor Yellow
    exit 1
}

# Create certificate directory
if (!(Test-Path $CertDir)) {
    New-Item -ItemType Directory -Path $CertDir | Out-Null
    Write-Host "📁 Created certificate directory: $CertDir" -ForegroundColor Green
}

Set-Location $CertDir

try {
    # Generate CA private key
    Write-Host "📋 Generating CA private key..." -ForegroundColor Cyan
    & openssl genrsa -out ca-key.pem $KeySize
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate CA private key" }

    # Generate CA certificate
    Write-Host "📋 Generating CA certificate..." -ForegroundColor Cyan
    $CASubject = "/C=$Country/ST=$State/L=$City/O=$Org/OU=$OU CA/CN=EMQX CA/emailAddress=$Email"
    & openssl req -new -x509 -days $Days -key ca-key.pem -out ca-cert.pem -subj $CASubject
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate CA certificate" }

    # Generate server private key
    Write-Host "📋 Generating server private key..." -ForegroundColor Cyan
    & openssl genrsa -out server-key.pem $KeySize
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate server private key" }

    # Generate server certificate signing request
    Write-Host "📋 Generating server CSR..." -ForegroundColor Cyan
    $ServerSubject = "/C=$Country/ST=$State/L=$City/O=$Org/OU=$OU/CN=$ServerCN/emailAddress=$Email"
    & openssl req -new -key server-key.pem -out server.csr -subj $ServerSubject
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate server CSR" }

    # Create server certificate extensions
    Write-Host "📋 Creating server certificate extensions..." -ForegroundColor Cyan
    $ServerExtContent = @"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
"@

    # Parse and add alternative names
    $AltNamesList = $ServerAltNames -split ","
    for ($i = 0; $i -lt $AltNamesList.Count; $i++) {
        $ServerExtContent += "`n$($i + 1)=$($AltNamesList[$i].Trim())"
    }

    $ServerExtContent | Out-File -FilePath "server-ext.cnf" -Encoding ASCII

    # Generate server certificate
    Write-Host "📋 Generating server certificate..." -ForegroundColor Cyan
    & openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -days $Days -extensions v3_req -extfile server-ext.cnf
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate server certificate" }

    # Generate client private key
    Write-Host "📋 Generating client private key..." -ForegroundColor Cyan
    & openssl genrsa -out client-key.pem $KeySize
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate client private key" }

    # Generate client certificate signing request
    Write-Host "📋 Generating client CSR..." -ForegroundColor Cyan
    $ClientSubject = "/C=$Country/ST=$State/L=$City/O=$Org/OU=$OU Client/CN=EMQX Client/emailAddress=$Email"
    & openssl req -new -key client-key.pem -out client.csr -subj $ClientSubject
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate client CSR" }

    # Generate client certificate
    Write-Host "📋 Generating client certificate..." -ForegroundColor Cyan
    & openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -days $Days
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate client certificate" }

    # Clean up temporary files
    Remove-Item -Path "server.csr", "client.csr", "server-ext.cnf" -ErrorAction SilentlyContinue

    Write-Host "✅ Certificate generation complete!" -ForegroundColor Green
    Write-Host ""
    Write-Host "📁 Generated files in $CertDir/:" -ForegroundColor Yellow
    Write-Host "   ca-cert.pem      - Certificate Authority certificate" -ForegroundColor White
    Write-Host "   ca-key.pem       - Certificate Authority private key" -ForegroundColor White
    Write-Host "   server-cert.pem  - Server certificate" -ForegroundColor White
    Write-Host "   server-key.pem   - Server private key" -ForegroundColor White
    Write-Host "   client-cert.pem  - Client certificate" -ForegroundColor White
    Write-Host "   client-key.pem   - Client private key" -ForegroundColor White
    Write-Host ""

    # Display certificate details
    Write-Host "🔒 Certificate details:" -ForegroundColor Yellow
    Write-Host "CA Certificate Subject:" -ForegroundColor Cyan
    & openssl x509 -in ca-cert.pem -text -noout | Select-String "Subject:" | ForEach-Object { Write-Host "   $($_.Line.Trim())" -ForegroundColor White }
    
    Write-Host "Server Certificate Subject:" -ForegroundColor Cyan
    & openssl x509 -in server-cert.pem -text -noout | Select-String "Subject:" | ForEach-Object { Write-Host "   $($_.Line.Trim())" -ForegroundColor White }
    
    Write-Host ""
    Write-Host "📅 Certificate validity:" -ForegroundColor Yellow
    Write-Host "CA Certificate:" -ForegroundColor Cyan
    & openssl x509 -in ca-cert.pem -noout -dates | ForEach-Object { Write-Host "   $_" -ForegroundColor White }
    
    Write-Host "Server Certificate:" -ForegroundColor Cyan
    & openssl x509 -in server-cert.pem -noout -dates | ForEach-Object { Write-Host "   $_" -ForegroundColor White }
    
    Write-Host ""
    Write-Host "🚀 Ready for production deployment!" -ForegroundColor Green

} catch {
    Write-Host "❌ Error during certificate generation: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    Set-Location ..
}