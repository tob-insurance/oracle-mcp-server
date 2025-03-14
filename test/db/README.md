# Test Database Setup

This directory contains a Docker setup for running an Oracle Free database for testing.

## Prerequisites

- Docker and Docker Compose
- Access to Oracle Container Registry (you need to accept the terms and login)

## Login to Oracle Container Registry

Before running the database, you need to login to the Oracle Container Registry:

```bash
docker login container-registry.oracle.com
```

## Starting the Database

From this directory, run:

```bash
docker-compose up -d
```

The database will take a few minutes to initialize. You can check the logs with:

```bash
docker-compose logs -f
```

## Connection Details

- Hostname: localhost
- Port: 1521
- Service Name: FREEPDB1
- Test User: testuser
- Test Password: testpass
- Connection String: testuser/testpass@//localhost:1521/FREEPDB1

## Sample Data

The database is initialized with the following tables:
- customers
- orders
- products
- order_items

These tables are populated with sample data that can be used for testing the database context provider.

## Stopping the Database

To stop the database:

```bash
docker-compose down
```

To remove all data and start fresh:

```bash
docker-compose down -v
```