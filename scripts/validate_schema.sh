# Create scripts/validate_schema.sh
cat > scripts/validate_schema.sh << 'EOF'
#!/bin/bash

echo "Validating schema structure..."
psql -h localhost -p 5435 -U postgres -d postgres << 'ENDSQL'
\dt openalex.*
\d+ openalex.works
\d+ openalex.authors
\d+ openalex.concepts
SELECT schemaname, tablename, tableowner 
FROM pg_tables 
WHERE schemaname = 'openalex' 
ORDER BY tablename;
ENDSQL
EOF

chmod +x scripts/validate_schema.sh
