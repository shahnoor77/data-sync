#!/bin/bash

# EMQX Deployment Script
# Deploys production-grade EMQX broker with TLS, authentication, and monitoring

set -e

# Configuration
COMPOSE_FILE="docker-compose.emqx.yaml"
ENV_FILE="emqx_config.env"
CERT_DIR="certs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check OpenSSL
    if ! command -v openssl &> /dev/null; then
        log_error "OpenSSL is not installed"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Generate certificates if they don't exist
setup_certificates() {
    log_info "Setting up TLS certificates..."
    
    if [ ! -d "$CERT_DIR" ] || [ ! -f "$CERT_DIR/ca-cert.pem" ]; then
        log_info "Generating TLS certificates..."
        chmod +x generate_certs.sh
        ./generate_certs.sh
        log_success "TLS certificates generated"
    else
        log_info "TLS certificates already exist"
        
        # Check certificate expiry
        if openssl x509 -checkend 2592000 -noout -in "$CERT_DIR/server-cert.pem" > /dev/null; then
            log_success "Server certificate is valid for at least 30 days"
        else
            log_warning "Server certificate expires within 30 days"
            read -p "Do you want to regenerate certificates? (y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                rm -rf "$CERT_DIR"
                ./generate_certs.sh
                log_success "TLS certificates regenerated"
            fi
        fi
    fi
}

# Validate configuration
validate_config() {
    log_info "Validating configuration..."
    
    if [ ! -f "$ENV_FILE" ]; then
        log_error "Configuration file $ENV_FILE not found"
        exit 1
    fi
    
    # Check required variables
    required_vars=("MQTT_USERNAME" "MQTT_PASSWORD" "BROKER_HOST")
    for var in "${required_vars[@]}"; do
        if ! grep -q "^$var=" "$ENV_FILE"; then
            log_error "Required variable $var not found in $ENV_FILE"
            exit 1
        fi
    done
    
    log_success "Configuration validation passed"
}

# Create Docker network if it doesn't exist
setup_network() {
    log_info "Setting up Docker network..."
    
    if ! docker network ls | grep -q "emqx_network"; then
        docker network create emqx_network
        log_success "Docker network created"
    else
        log_info "Docker network already exists"
    fi
}

# Build and start services
deploy_services() {
    log_info "Building and deploying EMQX services..."
    
    # Build the image
    docker-compose -f "$COMPOSE_FILE" build
    log_success "EMQX image built"
    
    # Start services
    docker-compose -f "$COMPOSE_FILE" up -d
    log_success "EMQX services started"
}

# Wait for EMQX to be ready
wait_for_emqx() {
    log_info "Waiting for EMQX to be ready..."
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker-compose -f "$COMPOSE_FILE" exec -T emqx emqx_ctl status > /dev/null 2>&1; then
            log_success "EMQX is ready"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    
    log_error "EMQX failed to start within expected time"
    return 1
}

# Setup authentication
setup_authentication() {
    log_info "Setting up authentication..."
    
    # Extract credentials from config
    USERNAME=$(grep "^MQTT_USERNAME=" "$ENV_FILE" | cut -d'=' -f2)
    PASSWORD=$(grep "^MQTT_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2)
    
    if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ]; then
        log_error "MQTT credentials not found in configuration"
        return 1
    fi
    
    # Add user to EMQX
    docker-compose -f "$COMPOSE_FILE" exec -T emqx emqx_ctl users add "$USERNAME" "$PASSWORD" > /dev/null 2>&1 || true
    
    log_success "Authentication configured for user: $USERNAME"
}

# Display deployment information
show_deployment_info() {
    log_success "EMQX Industrial Deployment Complete!"
    echo ""
    echo "🌐 Service Endpoints:"
    echo "   MQTT (Plain):      tcp://localhost:1883"
    echo "   MQTT (TLS):        ssl://localhost:8883"
    echo "   WebSocket:         ws://localhost:8083/mqtt"
    echo "   WebSocket (TLS):   wss://localhost:8084/mqtt"
    echo "   Dashboard:         http://localhost:18083"
    echo ""
    echo "🔐 Authentication:"
    USERNAME=$(grep "^MQTT_USERNAME=" "$ENV_FILE" | cut -d'=' -f2)
    echo "   Username: $USERNAME"
    echo "   Password: [configured in $ENV_FILE]"
    echo ""
    echo "📊 Monitoring:"
    echo "   docker-compose -f $COMPOSE_FILE logs -f emqx"
    echo "   docker-compose -f $COMPOSE_FILE exec emqx emqx_ctl status"
    echo ""
    echo "🧪 Testing:"
    echo "   python3 publisher.py --duration 60 --rate 100 --sensors 5"
    echo "   python3 subscriber.py --duration 60 --verbose"
    echo ""
    echo "🛠️  Management:"
    echo "   Start:   docker-compose -f $COMPOSE_FILE up -d"
    echo "   Stop:    docker-compose -f $COMPOSE_FILE down"
    echo "   Restart: docker-compose -f $COMPOSE_FILE restart"
    echo "   Logs:    docker-compose -f $COMPOSE_FILE logs -f"
}

# Health check
health_check() {
    log_info "Performing health check..."
    
    # Check container status
    if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
        log_error "EMQX container is not running"
        return 1
    fi
    
    # Check EMQX status
    if ! docker-compose -f "$COMPOSE_FILE" exec -T emqx emqx_ctl status > /dev/null 2>&1; then
        log_error "EMQX service is not responding"
        return 1
    fi
    
    # Check ports
    for port in 1883 8883 8083 8084 18083; do
        if ! nc -z localhost $port 2>/dev/null; then
            log_warning "Port $port is not accessible"
        fi
    done
    
    log_success "Health check passed"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up deployment..."
    docker-compose -f "$COMPOSE_FILE" down -v
    docker network rm emqx_network 2>/dev/null || true
    log_success "Cleanup complete"
}

# Main deployment function
main() {
    echo "🏭 EMQX Industrial Deployment"
    echo "=============================="
    
    case "${1:-deploy}" in
        "deploy")
            check_prerequisites
            setup_certificates
            validate_config
            setup_network
            deploy_services
            wait_for_emqx
            setup_authentication
            health_check
            show_deployment_info
            ;;
        "cleanup")
            cleanup
            ;;
        "health")
            health_check
            ;;
        "info")
            show_deployment_info
            ;;
        *)
            echo "Usage: $0 [deploy|cleanup|health|info]"
            echo ""
            echo "Commands:"
            echo "  deploy   - Deploy EMQX services (default)"
            echo "  cleanup  - Stop and remove all services"
            echo "  health   - Perform health check"
            echo "  info     - Show deployment information"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"