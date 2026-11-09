import os
import json
import boto3
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_json_from_s3(bucket_name: str, key: str):
    """Reads a JSON file from S3 and returns its content."""
    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        raise

def delivery_report(err, msg):
    """Callback function for Kafka message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def lambda_handler(event, context):
    """Lambda handler function to process and send simple payload to Kafka."""
    try:
        # Get configuration from environment variables or use defaults
        bootstrap_servers = os.environ.get('BOOTSTRAP_SERVERS', 'kafka.bian-iac-poc-cg.com:9092')
        topic = os.environ.get('TOPIC', 'simple_topic')
        schema_registry_url = os.environ.get('SCHEMA_REGISTRY_URL', 'http://a2d932ae0ea2b4afcb366a976e5dbfd7-1985348606.ap-southeast-2.elb.amazonaws.com')
        
        # S3 configuration
        s3_bucket = event.get("s3_bucket", "nz-deploy-dev-combined-storage")
        s3_key = event.get("s3_key", "single_test.json")
        
        logger.info(f"Reading payload from S3 bucket: {s3_bucket}, key: {s3_key}")
        
        # Read payload from S3 first
        payload = read_json_from_s3(s3_bucket, s3_key)
        logger.info(f"Successfully read payload from S3: {payload}")
        
        # Set up Schema Registry client
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Get schema for the topic from Schema Registry
        subject_name = f"{topic}-value"
        schema = schema_registry_client.get_latest_version(subject_name).schema
        logger.info(f"Retrieved schema from registry for subject: {subject_name}")
        
        # Create JSON serializer
        json_serializer = JSONSerializer(schema, schema_registry_client)
        
        # Kafka Producer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'PLAINTEXT'
        }
        producer = Producer(conf)
        logger.info(f"Created Kafka producer with bootstrap servers: {bootstrap_servers}")
        
        # Serialize the payload using the schema
        try:
            value_bytes = json_serializer(payload, SerializationContext(topic, MessageField.VALUE))
            logger.info("Successfully serialized payload with schema")
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            logger.error(f"Payload: {json.dumps(payload)}")
            raise
        
        # Send to Kafka using id as key if available, otherwise use a default
        key = str(payload.get("id", "default_key")).encode('utf-8')
        producer.produce(
            topic=topic,
            key=key,
            value=value_bytes,
            callback=delivery_report
        )
        logger.info(f"Produced message to topic {topic} with key: {key.decode('utf-8')}")
        
        # Ensure all messages are delivered
        producer.flush()
        logger.info("Producer flushed all messages")
        
        # Return success
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully sent payload to Kafka',
                'payload': payload
            })
        }
        
    except Exception as e:
        # Log the full error with stack trace
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        
        # Return error details
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }