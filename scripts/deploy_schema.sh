mkdir -p scripts
cat > scripts/deploy_schema.sh << 'EOF'
#!/bin/bash

# Configuration
PROJECT_ID="neudev01"
INSTANCE_NAME="openalex-db-eu"
DATABASE_NAME="postgres"

# Deploy schema using Cloud SQL Proxy
echo "Deploying schema to $INSTANCE_NAME..."
PGPASSWORD=$DB_PASSWORD psql \
  -h localhost \
  -p 5432 \
  -U postgres \
  -d $DATABASE_NAME \
  -f sql/schema/01_initialize_schema.sql

echo "Schema deployment completed."
EOF

chmod +x scripts/deploy_schema.sh

