"""
Firebase Configuration and Initialization Module
Handles Firebase connection management with proper error handling and singleton pattern.
"""
import os
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore, exceptions
from firebase_admin.firestore import Client as FirestoreClient
from google.cloud.firestore_v1.client import Client
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FirebaseConfig:
    """Configuration for Firebase connection"""
    project_id: str
    credentials_path: Optional[str] = None
    memory_collection: str = "agent_memories"
    max_batch_size: int = 100
    default_retention_days: int = 30


class FirebaseConnectionError(Exception):
    """Custom exception for Firebase connection failures"""
    pass


class FirebaseManager:
    """
    Singleton manager for Firebase connection.
    Ensures single connection instance and provides robust error handling.
    """
    _instance: Optional['FirebaseManager'] = None
    _client: Optional[FirestoreClient] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.config: Optional[FirebaseConfig] = None
            self._initialized = True
    
    def initialize(self, config: FirebaseConfig) -> None:
        """
        Initialize Firebase connection with provided configuration.
        
        Args:
            config: FirebaseConfig object with connection parameters
            
        Raises:
            FirebaseConnectionError: If initialization fails
            ValueError: If required configuration is missing
        """
        try:
            # Validate configuration
            if not config.project_id:
                raise ValueError("Firebase project_id is required")
            
            self.config = config
            logger.info(f"Initializing Firebase connection to project: {config.project_id}")
            
            # Initialize Firebase app
            if firebase_admin._apps:
                # App already exists, get existing app
                app = firebase_admin.get_app()
                logger.info("Using existing Firebase app instance")
            else:
                # Create new app instance
                if config.credentials_path and os.path.exists(config.credentials_path):
                    cred = credentials.Certificate(config.credentials_path)
                    firebase_admin.initialize_app(cred, {
                        'projectId': config.project_id
                    })
                    logger.info(f"Initialized Firebase with service account: {config.credentials_path}")
                else:
                    # Use default credentials (for environments like Google Cloud Run, App Engine)
                    firebase_admin.initialize_app(options={
                        'projectId': config.project_id
                    })
                    logger.info("Initialized Firebase with default application credentials")
            
            # Get Firestore client
            self._client = firestore.client()
            logger.info("Firebase Firestore client initialized successfully")
            
        except exceptions.FirebaseError as e:
            error_msg = f"Firebase initialization error: {str(e)}"
            logger.error(error_msg)
            raise FirebaseConnectionError(error_msg) from e
        except ValueError as e:
            error_msg = f"Configuration error: {str(e)}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during Firebase initialization: {str(e)}"
            logger.error(error_msg)
            raise FirebaseConnectionError(error_msg) from e
    
    @property
    def client(self) -> FirestoreClient:
        """
        Get Firestore client instance.
        
        Returns:
            FirestoreClient