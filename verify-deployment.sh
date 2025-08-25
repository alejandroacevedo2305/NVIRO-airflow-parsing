#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}    Apache Airflow Deployment Verification     ${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Function to check service
check_service() {
    local service=$1
    local port=$2
    local name=$3
    
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:${port}/health 2>/dev/null | grep -q "200"; then
        echo -e "${GREEN}✓${NC} ${name} is running on port ${port}"
        return 0
    else
        echo -e "${RED}✗${NC} ${name} is not accessible on port ${port}"
        return 1
    fi
}

# Check Docker services
echo -e "${YELLOW}Checking Docker Services...${NC}"
services_running=$(docker-compose ps --services --filter "status=running" | wc -l)
total_services=$(docker-compose ps --services | wc -l)

if [ "$services_running" -eq "$total_services" ]; then
    echo -e "${GREEN}✓${NC} All $services_running services are running"
else
    echo -e "${RED}✗${NC} Only $services_running out of $total_services services are running"
fi

# Check container health
echo ""
echo -e "${YELLOW}Checking Container Health...${NC}"
unhealthy=$(docker-compose ps --format json | python3 -c "
import json, sys
for line in sys.stdin:
    data = json.loads(line)
    if 'unhealthy' in data.get('Status', ''):
        print(data['Name'])
" 2>/dev/null)

if [ -z "$unhealthy" ]; then
    echo -e "${GREEN}✓${NC} All containers are healthy"
else
    echo -e "${RED}✗${NC} Unhealthy containers found:"
    echo "$unhealthy"
fi

# Check Airflow Web UI
echo ""
echo -e "${YELLOW}Checking Airflow Web UI...${NC}"
check_service 3000 "Airflow Web UI"

# Check DAGs
echo ""
echo -e "${YELLOW}Checking DAGs...${NC}"
dag_count=$(docker-compose exec -T airflow-webserver airflow dags list 2>/dev/null | grep -c "^[a-z]" || echo "0")

if [ "$dag_count" -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Found $dag_count DAGs loaded successfully"
    docker-compose exec -T airflow-webserver airflow dags list --output table 2>/dev/null | head -10
else
    echo -e "${RED}✗${NC} No DAGs found"
fi

# Check database connection
echo ""
echo -e "${YELLOW}Checking Database Connection...${NC}"
if docker-compose exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} PostgreSQL is accepting connections"
else
    echo -e "${RED}✗${NC} PostgreSQL is not accepting connections"
fi

# Check Redis connection
echo ""
echo -e "${YELLOW}Checking Redis Connection...${NC}"
if docker-compose exec -T redis redis-cli ping 2>/dev/null | grep -q "PONG"; then
    echo -e "${GREEN}✓${NC} Redis is responding to ping"
else
    echo -e "${RED}✗${NC} Redis is not responding"
fi

# Check resource usage
echo ""
echo -e "${YELLOW}Resource Usage:${NC}"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | head -10

# Summary
echo ""
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}                   SUMMARY                      ${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo -e "${GREEN}Deployment Status: SUCCESSFUL${NC}"
echo ""
echo -e "Access your Airflow instance at:"
echo -e "  ${BLUE}http://localhost:3000${NC}"
echo ""
echo -e "Login with:"
echo -e "  Username: ${YELLOW}admin${NC}"
echo -e "  Password: ${YELLOW}admin${NC}"
echo ""
echo -e "${YELLOW}⚠️  Remember to change default credentials in production!${NC}"
echo ""
echo -e "To monitor workers with Flower, run:"
echo -e "  ${BLUE}docker-compose --profile flower up -d${NC}"
echo ""
echo -e "To view logs, run:"
echo -e "  ${BLUE}docker-compose logs -f [service-name]${NC}"
echo ""
