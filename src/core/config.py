"""
Configuration management for Neo4j knowledge graph construction.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv


class Config(BaseSettings):
    """Configuration settings loaded from environment variables."""
    
    # Neo4j connection settings
    neo4j_uri: str = Field(..., description="Neo4j database URI")
    neo4j_username: str = Field(..., description="Neo4j username")
    neo4j_password: str = Field(..., description="Neo4j password")
    neo4j_database: str = Field(default="neo4j", description="Neo4j database name")
    
    # Data file paths
    chalmers_org_file: str = Field(
        default="/Users/filipberntsson/Dev/small_graph_demo/chalmers_organizational_structure.json",
        description="Path to Chalmers organizational structure JSON file"
    )
    
    # Logging and debugging
    log_level: str = Field(default="INFO", description="Logging level")
    debug_mode: bool = Field(default=False, description="Enable debug mode")
    
    class Config:
        env_file = ".env"
        env_prefix = ""
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env

    @classmethod
    def load_from_env(cls, env_file: Optional[str] = None) -> 'Config':
        """Load configuration from environment file."""
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        return cls()

    def validate_neo4j_connection(self) -> bool:
        """Validate that all required Neo4j connection settings are present."""
        required_fields = ['neo4j_uri', 'neo4j_username', 'neo4j_password']
        return all(getattr(self, field) for field in required_fields)

    def get_neo4j_auth(self) -> tuple:
        """Get Neo4j authentication tuple."""
        return (self.neo4j_username, self.neo4j_password)


# Global configuration instance
config = Config.load_from_env()