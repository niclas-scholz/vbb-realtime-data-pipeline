# vbb-realtime-data-pipeline



# General Goal:
VBB GTFS-RT --> Kafka --> Spark Structured Streaming --> Delta Medallion (Bronze, Silver, Gold) --> delay analytics and visualization (mb. Dashboard)


# Sources:
RT: https://production.gtfsrt.vbb.de/data (60 req/min)

Static: https://unternehmen.vbb.de/en/digital-services/datasets (weekly)


"docker compose up" to start

Kafka UI: http://localhost:8080 · Spark UI: http://localhost:8081 · Dashboard: http://localhost:8501
