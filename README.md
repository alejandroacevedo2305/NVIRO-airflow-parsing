# Apache Airflow Docker Setup

A production-ready Apache Airflow deployment using Docker Compose with CeleryExecutor, Redis, and PostgreSQL.

## 🚀 Quick Start

### Prerequisites

- Docker Engine 20.10+ ([Install Docker](https://docs.docker.com/get-docker/))
- Docker Compose 2.0+ ([Install Docker Compose](https://docs.docker.com/compose/install/))
- Minimum 4GB RAM available for Docker
- 10GB free disk space

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/NVIRO-airflow-parsing.git
   cd NVIRO-airflow-parsing
   ```

2. **Run the initialization script:**
   ```bash
   ./init-airflow.sh
   ```

   This script will:
   - Check Docker installation
   - Create necessary directories
   - Set up environment variables
   - Build the custom Airflow image
   - Initialize the Airflow database

3. **Start Airflow:**
   ```bash
   docker compose up -d
   ```

4. **Access Airflow UI:**

   Open your browser and navigate to: http://localhost:3000

   Default credentials:
   - Username: `admin`
   - Password: `admin`

## 📁 Project Structure

```
NVIRO-airflow-parsing/
├── dags/               # Your Airflow DAGs go here
├── logs/               # Airflow logs (auto-created)
├── plugins/            # Custom Airflow plugins
├── data/               # Data directory for processing
├── docker-compose.yaml # Docker Compose configuration
├── Dockerfile          # Custom Airflow image
├── env.example         # Example environment variables
├── init-airflow.sh     # Initialization script
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## 🎯 Features

- **Production-Ready**: Optimized for production use with proper resource limits and health checks
- **Scalable**: Uses CeleryExecutor with Redis for distributed task execution
- **Persistent Storage**: PostgreSQL database for metadata storage
- **Custom Port**: Airflow UI runs on port 3000 (configurable)
- **Auto-Discovery**: DAGs in the `dags/` folder are automatically detected
- **Health Monitoring**: Built-in health checks for all services
- **Security**: Fernet key encryption and configurable authentication

## 📝 Configuration

### Environment Variables

Copy `env.example` to `.env` and modify as needed:

```bash
cp env.example .env
```

Key variables to configure:

- `AIRFLOW_FERNET_KEY`: Encryption key (generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
- `AIRFLOW_SECRET_KEY`: Webserver secret key
- `_AIRFLOW_WWW_USER_USERNAME`: Admin username
- `_AIRFLOW_WWW_USER_PASSWORD`: Admin password

### Adding Python Dependencies

Add your Python packages to `requirements.txt`:

```txt
pandas==2.0.3
numpy==1.24.3
requests==2.31.0
your-package==1.0.0
```

Then rebuild the image:

```bash
docker compose build
docker compose up -d
```

## 🚦 Managing Airflow

### Start Services
```bash
docker compose up -d
```

### Stop Services
```bash
docker compose down
```

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-webserver
```

### Scale Workers
```bash
docker compose up -d --scale airflow-worker=3
```

### Execute Airflow CLI Commands
```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow tasks list <dag_id>
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

### Clean Up Everything (including volumes)
```bash
docker compose down -v
```

## 📊 Monitoring

### Flower (Celery Monitoring)

Enable Flower for monitoring Celery workers:

```bash
docker compose --profile flower up -d
```

Access Flower at: http://localhost:5555

### Health Checks

All services include health checks. Check service health:

```bash
docker compose ps
```

## 🔧 Troubleshooting

### Permission Issues
If you encounter permission issues:

```bash
sudo chown -R $(id -u):$(id -g) ./dags ./logs ./plugins ./data
```

### Database Initialization & Connection Issues

- Error: `You need to initialize the database. Please run "airflow db init"`.

  Fix (one-time init):
  ```bash
  ./init-airflow.sh
  # or
  docker compose build
  docker compose up --exit-code-from airflow-init airflow-init
  ```

  Then start services:
  ```bash
  docker compose up -d
  ```

- If init still fails, force-initialize inside a container:
  ```bash
  docker compose run --rm airflow-webserver airflow db init
  docker compose up -d
  ```

- Inspect init logs when diagnosing:
  ```bash
  docker compose logs --no-color airflow-init | tail -n 200
  ```

### Restore/Force Admin Credentials
If the default admin credentials are missing or changed, recreate/update them:
```bash
docker compose run --rm airflow-webserver \
  airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || \
docker compose run --rm airflow-webserver \
  airflow users update \
  --username admin \
  --password admin
```
Then restart the webserver if needed:
```bash
docker compose restart airflow-webserver
```

### Start Services Separately (after init)
The `init-airflow.sh` script prepares and initializes the environment but does
not start the containers. Start them separately with:
```bash
docker compose up -d
```

### Ensure Correct Airflow User (Linux)
Make sure your `.env` contains a valid `AIRFLOW_UID` to avoid permission issues
on mounted volumes:
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

## 📚 Creating DAGs

Place your DAG files in the `dags/` directory. Example DAG:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def print_hello():
    return 'Hello Airflow!'

task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the [Apache Airflow documentation](https://airflow.apache.org/docs/)
- Join the [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/)

## 🔗 Useful Links

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Best Practices for Airflow](https://airflow.apache-airflow/stable/best-practices.html)
