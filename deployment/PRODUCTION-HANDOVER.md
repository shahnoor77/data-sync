# EMQX 5.x Production Deployment - Railway

## LIVE ENDPOINTS
- **MQTT TCP**: caboose.proxy.rlwy.net:34943
- **Dashboard**: http://emqx-broker-production.up.railway.app:18083
- **Docker Image**: shahnoor77/emqx-broker:v1

## DEPLOYMENT STATUS
✅ EMQX 5.4.1 running on Railway  
✅ TLS certificates loaded via environment variables 
✅ Health checks passing  
✅ Production-ready configuration  

## REQUIRED ENVIRONMENT VARIABLES
Set these in Railway dashboard:
```
EMQX_SSL_CA_B64=<base64_ca_certificate>
EMQX_SSL_CERT_B64=<base64_server_certificate>  
EMQX_SSL_KEY_B64=<base64_server_private_key>
EMQX_NODE__COOKIE=<secure_random_string>
EMQX_DASHBOARD__DEFAULT_USERNAME=<username>
EMQX_DASHBOARD__DEFAULT_PASSWORD=<secure_password>
```

## TESTING
1. Access dashboard at live URL
2. Connect MQTT client to caboose.proxy.rlwy.net:34943
3. Verify TLS on port 8883 (if needed)

## SECURITY
- All secrets in Railway environment variables
- TLS certificates loaded at runtime
- No hardcoded credentials
- Certificates excluded from Git via .gitignore

## FILES
- `Dockerfile.secure`: Main deployment file
- `entrypoint.sh`: Certificate loading script  
- `emqx.conf`: EMQX configuration
- `railway.json`: Railway deployment config
- `tunables.env`: Environment variable template