import os
import json
import random
import string
import boto3
import logging
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def generate_random_party_record(template, hardcoded_user_id):
    """Generate random party record with hardcoded usrId for consistent user journey"""
    logger.info("Starting to generate random party record with hardcoded usrId")
    
    try:
        # Deep copy the template to avoid modifying the original
        record = json.loads(json.dumps(template))
        
        # Get event type for logging
        event_type = record.get('type', 'unknown')
        logger.info(f"Processing event type: {event_type}")
        
        # Use hardcoded usrId for consistent user journey
        logger.info(f"Using hardcoded usrId: {hardcoded_user_id}")
        
        # Update usrId in the record structure
        if 'data' in record and 'adtDtl' in record['data']:
            record['data']['adtDtl']['usrId'] = hardcoded_user_id
            logger.info(f"Updated usrId in data.adtDtl.usrId: {hardcoded_user_id}")
        
        # Also update fisuserid at root level to match
        record['fisuserid'] = hardcoded_user_id
        logger.info(f"Updated fisuserid at root level: {hardcoded_user_id}")
        
        # Generate random values for correlation fields (but keep usrId consistent)
        new_correlation_id = ''.join(random.choices(string.ascii_letters + string.digits + '-', k=36))
        new_id = ''.join(random.choices(string.ascii_letters + string.digits + '-', k=36))
        
        record['id'] = new_id
        record['fiscorrelationid'] = new_correlation_id
        
        # Update msgUuid in audit details to match fiscorrelationid
        if 'data' in record and 'adtDtl' in record['data']:
            record['data']['adtDtl']['msgUuid'] = new_correlation_id
        
        # Update timestamps to current time
        current_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        record["time"] = current_time
        if 'data' in record and 'adtDtl' in record['data']:
            record["data"]["adtDtl"]["operTs"] = current_time
        
        # Generate random partition key (but keep it consistent for same user if needed)
        record['partitionkey'] = ''.join(random.choices(string.digits, k=9))
        record['fiscourierpartitionkey'] = record['partitionkey']
        
        # Update source path with new correlation ID
        if 'source' in record and new_correlation_id:
            # Replace the correlation ID in the source path
            source_parts = record['source'].split('/')
            if len(source_parts) > 0:
                source_parts[-1] = new_correlation_id
                record['source'] = '/'.join(source_parts)
        
        logger.info("Successfully generated random party record")
        logger.info(f"Event type: {event_type}")
        logger.info(f"Hardcoded usrId: {hardcoded_user_id}")
        logger.info(f"Generated correlation ID: {new_correlation_id}")
        
        return record
        
    except Exception as e:
        logger.error(f"Error generating random party record: {str(e)}")
        raise e

def extract_user_id_from_record(record, hardcoded_user_id):
    """Extract usrId from the record for use as Kafka message key"""
    logger.info("Extracting usrId for Kafka message key")
    
    try:
        # First try to get from audit details
        if 'data' in record and 'adtDtl' in record['data']:
            user_id = record['data']['adtDtl'].get('usrId', '')
            if user_id:
                logger.info(f"Extracted usrId from data.adtDtl.usrId: {user_id}")
                return user_id
        
        # Fallback to fisuserid at root level
        if 'fisuserid' in record:
            user_id = record['fisuserid']
            logger.info(f"Extracted usrId from fisuserid: {user_id}")
            return user_id
        
        # Final fallback to hardcoded value
        logger.info(f"Using hardcoded usrId as fallback: {hardcoded_user_id}")
        return hardcoded_user_id
        
    except Exception as e:
        logger.error(f"Error extracting user ID: {str(e)}")
        return hardcoded_user_id

def read_json_from_s3(bucket_name, key):
    """Reads a JSON file from S3 and returns its content."""
    logger.info(f"Starting to read JSON file from S3: s3://{bucket_name}/{key}")
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read()
        
        logger.info(f"Successfully read {len(content)} bytes from S3")
        
        json_content = json.loads(content)
        logger.info(f"Successfully parsed JSON content. Found {len(json_content)} events")
        
        return json_content
        
    except Exception as e:
        logger.error(f"Error reading JSON from S3: {str(e)}")
        raise e

def delivery_report(err, msg):
    """Delivery report callback for Kafka producer"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')

def lambda_handler(event, context):
    """Main Lambda handler function with hardcoded usrId for user journey flow"""
    logger.info("Lambda Party V3 function started - User Journey Flow")
    
    # Hardcoded usrId for consistent user journey across all events
    HARDCODED_USER_ID = "MXBUSER11123"
    logger.info(f"Using hardcoded usrId for user journey: {HARDCODED_USER_ID}")
    
    # Configuration - using updated configs
    s3_bucket = event.get("s3_bucket", "nz-deploy-dev-combined-storage")
    #s3_key = event.get("s3_key", "Payload_party.json")
    
    # Payload selection for user journey flow - uncomment the desired payload
    #s3_key = event.get("s3_key", "party/payloads/payload_cust_create.json")     # Step 1: User Creation
    #s3_key = event.get("s3_key", "party/payloads/payload_cust_change.json")   # Step 2: User Update
    # s3_key = event.get("s3_key", "party/payloads/payload_cust_rel_create.json") # Step 3: Account Relationship
    #s3_key = event.get("s3_key", "party/payloads/payload_cust_watch_sts_chg.json") # Step 4: Watchlist Status
    #s3_key = event.get("s3_key", "party/payloads/payload_cust_fatcha.json")   # Step 5: FATCA Update
    s3_key = event.get("s3_key", "party/payloads/Payload_party.json")  
    
    bootstrap_servers = event.get("bootstrap_servers", 'kafka.bian-iac-poc-cg.com:9092')
    topic = event.get("topic", 'customer_source1')
    schema_registry_url = event.get("schema_registry_url", 'http://a51e2f96779094e078b8f8308d54431f-1894914849.ap-southeast-2.elb.amazonaws.com')
    
    # Get event index (default to 0 for first event)
    event_index = event.get("event_index", 0)
    
    logger.info(f"Configuration:")
    logger.info(f"  S3 Bucket: {s3_bucket}")
    logger.info(f"  S3 Key: {s3_key}")
    logger.info(f"  Event Index: {event_index}")
    logger.info(f"  Kafka Topic: {topic}")
    logger.info(f"  Schema Registry URL: {schema_registry_url}")
    logger.info(f"  Hardcoded User ID: {HARDCODED_USER_ID}")
    
    try:
        # Step 1: Read event templates from S3
        logger.info("Step 1: Reading event templates from S3")
        templates = read_json_from_s3(s3_bucket, s3_key)
        
        # Step 2: Select event based on index
        logger.info("Step 2: Selecting event template by index")
        if not templates or len(templates) == 0:
            raise ValueError("No event templates found in S3 file")
        
        if event_index < 0 or event_index >= len(templates):
            raise ValueError(f"Invalid event index: {event_index}. Must be between 0 and {len(templates) - 1}")
        
        template = templates[event_index]
        event_type = template.get('type', 'Unknown')
        logger.info(f"Selected event type: {event_type}")
        
        # Step 3: Generate random record with hardcoded usrId
        logger.info("Step 3: Generating random record with hardcoded usrId for user journey")
        record = generate_random_party_record(template, HARDCODED_USER_ID)
        
        logger.info(f"Generated record for user journey:")
        logger.info(f"  Event type: {event_type}")
        logger.info(f"  User ID: {HARDCODED_USER_ID}")
        logger.info(f"  Event ID: {record.get('id', 'N/A')}")
        logger.info(f"  Correlation ID: {record.get('fiscorrelationid', 'N/A')}")
        
        # Step 4: Connect to Schema Registry
        logger.info("Step 4: Connecting to Schema Registry")
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Get the latest schema for the topic
        subject_name = f"{topic}-value"
        logger.info(f"Fetching schema for subject: {subject_name}")
        
        try:
            latest_schema = schema_registry_client.get_latest_version(subject_name)
            logger.info(f"Successfully retrieved schema version {latest_schema.version} for subject {subject_name}")
        except Exception as e:
            logger.error(f"Error fetching schema from registry: {str(e)}")
            logger.error(f"Attempted subject: {subject_name}")
            raise e
        
        # Step 5: Create JSON serializer with schema validation
        logger.info("Step 5: Creating JSON serializer with schema validation")
        json_serializer = JSONSerializer(latest_schema.schema.schema_str, schema_registry_client)
        
        # Step 6: Configure Kafka producer
        logger.info("Step 6: Configuring Kafka producer")
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'lambda-party-v3-user-journey-producer'
        }
        producer = Producer(producer_conf)
        
        # Step 7: Extract usrId for message key (should be our hardcoded value)
        logger.info("Step 7: Extracting usrId for Kafka message key")
        message_key = extract_user_id_from_record(record, HARDCODED_USER_ID)
        logger.info(f"Using message key (usrId): {message_key}")
        
        # Verify the key matches our hardcoded value
        if message_key != HARDCODED_USER_ID:
            logger.warning(f"Message key ({message_key}) doesn't match hardcoded usrId ({HARDCODED_USER_ID})")
        
        # Step 8: Validate and serialize the record
        logger.info("Step 8: Validating record against schema and serializing")
        try:
            serialized_value = json_serializer(record, SerializationContext(topic, MessageField.VALUE))
            logger.info("Record successfully validated against schema and serialized")
        except Exception as e:
            logger.error(f"Schema validation or serialization failed: {str(e)}")
            logger.error(f"Event type: {event_type}")
            logger.error(f"Schema subject used: {subject_name}")
            logger.error(f"Record content: {json.dumps(record, indent=2)}")
            raise e
        
        # Step 9: Publish to Kafka
        logger.info("Step 9: Publishing message to Kafka topic")
        try:
            # Convert message key to bytes
            key_bytes = message_key.encode('utf-8')
            
            producer.produce(
                topic=topic,
                key=key_bytes,
                value=serialized_value,
                callback=delivery_report
            )
            
            # Wait for message delivery
            producer.flush()
            logger.info(f"Successfully published 1 message to Kafka topic: {topic}")
            logger.info(f"Message key (usrId): {message_key} (ensures all user events go to same partition)")
            
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {str(e)}")
            raise e
        
        # Success response
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "message": f"1 message sent to Kafka topic: {topic}",
                "user_journey_flow": {
                    "hardcoded_user_id": HARDCODED_USER_ID,
                    "event_type": event_type,
                    "event_sequence": "User journey flow event",
                    "message_key": message_key,
                    "ensures_partition_consistency": True
                },
                "event_details": {
                    "event_index": event_index,
                    "s3_key": s3_key,
                    "schema_version": latest_schema.version,
                    "event_id": record.get('id', 'N/A'),
                    "correlation_id": record.get('fiscorrelationid', 'N/A')
                }
            })
        }
        
        logger.info("Lambda Party V3 User Journey function completed successfully")
        logger.info(f"User {HARDCODED_USER_ID} event {event_type} processed and sent to Kafka")
        return response
        
    except Exception as e:
        logger.error(f"Lambda Party V3 function failed: {str(e)}")
        
        error_response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": str(e),
                "user_journey_context": {
                    "hardcoded_user_id": HARDCODED_USER_ID,
                    "event_index": event.get("event_index", 0),
                    "s3_key": s3_key
                }
            })
        }
        
        return error_response