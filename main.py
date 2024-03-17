import json
import csv
from avro import datafile, io
from kafka import KafkaProducer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to ingest JSON data into Kafka topic
def ingest_json(file_path, kafka_topic, bootstrap_servers='localhost:9092'):
    """Ingest JSON data into Kafka topic."""
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                data = json.loads(line)
                producer.send(kafka_topic, value=data)
        logger.info(f"Successfully ingested JSON data from {file_path} into Kafka topic {kafka_topic}")
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"Error occurred while ingesting JSON data: {e}")
    finally:
        producer.close()

# Function to ingest CSV data into Kafka topic
def ingest_csv(file_path, kafka_topic, bootstrap_servers='localhost:9092'):
    """Ingest CSV data into Kafka topic."""
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                json_row = json.dumps(row)
                producer.send(kafka_topic, value=json_row)
        logger.info(f"Successfully ingested CSV data from {file_path} into Kafka topic {kafka_topic}")
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"Error occurred while ingesting CSV data: {e}")
    finally:
        producer.close()

# Function to ingest Avro data into Kafka topic
def ingest_avro(file_path, kafka_topic, bootstrap_servers='localhost:9092'):
    """Ingest Avro data into Kafka topic."""
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    
    try:
        with open(file_path, 'rb') as f:
            reader = datafile.DataFileReader(f, io.DatumReader())
            for record in reader:
                json_record = json.dumps(record)
                producer.send(kafka_topic, value=json_record)
        logger.info(f"Successfully ingested Avro data from {file_path} into Kafka topic {kafka_topic}")
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"Error occurred while ingesting Avro data: {e}")
    finally:
        producer.close()

# Placeholder for processing and correlation steps
def process_and_correlate_data():
    """Process and correlate data."""
    # Placeholder for processing steps
    logger.info("Performing processing steps...")
    # Placeholder for correlation steps
    logger.info("Performing correlation steps...")

# Main function for dynamic execution
def main(config_file):
    """Main function for dynamic execution."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file {config_file} not found.")
        return

    for source in config['data_sources']:
        if source['type'] == 'json':
            ingest_json(source['file_path'], source['kafka_topic'])
        elif source['type'] == 'csv':
            ingest_csv(source['file_path'], source['kafka_topic'])
        elif source['type'] == 'avro':
            ingest_avro(source['file_path'], source['kafka_topic'])

    process_and_correlate_data()

if __name__ == "__main__":
    config_file_path = 'config.json'
    main(config_file_path)
