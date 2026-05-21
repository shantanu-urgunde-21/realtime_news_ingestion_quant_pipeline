"""
Dummy Firebase Admin SDK - Local Testing without Real Firebase
"""
import logging
import uuid

logger = logging.getLogger(__name__)

class MockCredentials:
    def Certificate(self, path):
        logger.info(f"[DUMMY FIREBASE] Loaded mock credentials from {path}")
        return {"path": path}

credentials = MockCredentials()

def initialize_app(cred):
    logger.info("[DUMMY FIREBASE] Initialized mock Firebase app")
    return {"initialized": True}

class MockMessage:
    def __init__(self, topic=None, data=None):
        self.topic = topic
        self.data = data
        logger.debug(f"[DUMMY FIREBASE] Created mock message for topic '{topic}'")

class MockMessaging:
    Message = MockMessage
    
    def send(self, message):
        msg_id = f"mock-msg-{uuid.uuid4()}"
        logger.info(f"[DUMMY FIREBASE] Sent mock message to topic '{message.topic}'. ID: {msg_id}")
        return msg_id

messaging = MockMessaging()
