#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}       Apache Airflow Docker Setup Script      ${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Docker installation
if ! command_exists docker; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check Docker Compose installation
if ! command_exists docker compose && ! docker compose version >/dev/null 2>&1; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    echo "Please install Docker Compose first: https://docs.docker.com/compose/install/"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}Error: Docker daemon is not running${NC}"
    echo "Please start Docker and try again"
    exit 1
fi

# Set AIRFLOW_UID
echo -e "${YELLOW}Setting up environment...${NC}"
export AIRFLOW_UID=$(id -u)

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    if [ -f env.example ]; then
        echo -e "${YELLOW}Creating .env file from env.example...${NC}"
        cp env.example .env

        # Update AIRFLOW_UID in .env
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/AIRFLOW_UID=.*/AIRFLOW_UID=${AIRFLOW_UID}/" .env
        else
            # Linux
            sed -i "s/AIRFLOW_UID=.*/AIRFLOW_UID=${AIRFLOW_UID}/" .env
        fi

        echo -e "${GREEN}✓ .env file created${NC}"
    else
        echo -e "${RED}Warning: env.example not found${NC}"
        echo "AIRFLOW_UID=${AIRFLOW_UID}" > .env
    fi
else
    echo -e "${GREEN}✓ .env file already exists${NC}"
fi

# Create necessary directories
echo -e "${YELLOW}Creating required directories...${NC}"
mkdir -p ./dags ./logs ./plugins ./data

# Set proper permissions
echo -e "${YELLOW}Setting permissions...${NC}"
chmod -R 755 ./dags ./logs ./plugins ./data

echo -e "${GREEN}✓ Directories created and permissions set${NC}"

# Create requirements.txt if it doesn't exist
if [ ! -f requirements.txt ]; then
    echo -e "${YELLOW}Creating requirements.txt...${NC}"
    cat > requirements.txt << EOF
# Add your Python dependencies here
# pandas==2.0.3
# numpy==1.24.3
# requests==2.31.0
EOF
    echo -e "${GREEN}✓ requirements.txt created${NC}"
fi

# Build custom image
echo -e "${YELLOW}Building custom Airflow image...${NC}"
docker compose build

# Initialize Airflow database
echo -e "${YELLOW}Initializing Airflow...${NC}"
docker compose up airflow-init

# Check initialization status
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Airflow initialized successfully${NC}"
else
    echo -e "${RED}Error during Airflow initialization${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}        Setup completed successfully!          ${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo -e "${YELLOW}To start Airflow, run:${NC}"
echo -e "  ${GREEN}docker compose up -d${NC}"
echo ""
echo -e "${YELLOW}To stop Airflow, run:${NC}"
echo -e "  ${GREEN}docker compose down${NC}"
echo ""
echo -e "${YELLOW}Airflow will be available at:${NC}"
echo -e "  ${GREEN}http://localhost:3000${NC}"
echo ""
echo -e "${YELLOW}Default credentials:${NC}"
echo -e "  Username: ${GREEN}admin${NC}"
echo -e "  Password: ${GREEN}admin${NC}"
echo ""
echo -e "${RED}⚠️  IMPORTANT: Change the default credentials and security keys in production!${NC}"
