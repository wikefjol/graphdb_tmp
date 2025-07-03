"""
Centralized Neo4j client for knowledge graph operations.
"""

import logging
from typing import Dict, List, Any, Optional
from contextlib import contextmanager
from neo4j import GraphDatabase, Session
from neo4j.exceptions import ServiceUnavailable, AuthError

from .config import Config

logger = logging.getLogger(__name__)


class Neo4jClient:
    """Centralized Neo4j database client with connection management."""
    
    def __init__(self, config: Config):
        """Initialize Neo4j client with configuration."""
        self.config = config
        self.driver = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to Neo4j database."""
        try:
            if not self.config.validate_neo4j_connection():
                raise ValueError("Missing required Neo4j connection settings")
            
            self.driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=self.config.get_neo4j_auth()
            )
            
            # Test connection
            with self.driver.session(database=self.config.neo4j_database) as session:
                session.run("RETURN 1").consume()
            
            logger.info(f"Successfully connected to Neo4j at {self.config.neo4j_uri}")
            
        except AuthError as e:
            logger.error(f"Neo4j authentication failed: {e}")
            raise
        except ServiceUnavailable as e:
            logger.error(f"Neo4j service unavailable: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    @contextmanager
    def session(self, **kwargs):
        """Context manager for Neo4j session."""
        if not self.driver:
            raise RuntimeError("Neo4j driver not initialized")
        
        session_kwargs = {"database": self.config.neo4j_database}
        session_kwargs.update(kwargs)
        
        session = self.driver.session(**session_kwargs)
        try:
            yield session
        finally:
            session.close()
    
    def execute_query(self, query: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """Execute a query and return results as list of dictionaries."""
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(self, query: str, parameters: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a write query and return summary statistics."""
        with self.session() as session:
            result = session.run(query, parameters or {})
            summary = result.consume()
            return {
                'nodes_created': summary.counters.nodes_created,
                'nodes_deleted': summary.counters.nodes_deleted,
                'relationships_created': summary.counters.relationships_created,
                'relationships_deleted': summary.counters.relationships_deleted,
                'properties_set': summary.counters.properties_set,
                'labels_added': summary.counters.labels_added,
                'indexes_added': summary.counters.indexes_added,
                'constraints_added': summary.counters.constraints_added
            }
    
    def clear_database(self) -> Dict[str, Any]:
        """Clear all nodes and relationships from the database."""
        logger.warning("Clearing entire Neo4j database")
        
        # First clear relationships, then nodes
        clear_rels_query = "MATCH ()-[r]->() DELETE r"
        clear_nodes_query = "MATCH (n) DELETE n"
        
        with self.session() as session:
            # Clear relationships first
            rel_result = session.run(clear_rels_query)
            rel_summary = rel_result.consume()
            
            # Then clear nodes
            node_result = session.run(clear_nodes_query)
            node_summary = node_result.consume()
            
            total_summary = {
                'relationships_deleted': rel_summary.counters.relationships_deleted,
                'nodes_deleted': node_summary.counters.nodes_deleted
            }
            
            logger.info(f"Database cleared: {total_summary}")
            return total_summary
    
    def create_constraints(self, constraints: List[str]) -> int:
        """Create database constraints."""
        constraints_created = 0
        
        with self.session() as session:
            for constraint in constraints:
                try:
                    result = session.run(constraint)
                    summary = result.consume()
                    if summary.counters.constraints_added > 0:
                        constraints_created += 1
                        logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    logger.warning(f"Constraint already exists or failed: {constraint} - {e}")
        
        return constraints_created
    
    def create_indexes(self, indexes: List[str]) -> int:
        """Create database indexes."""
        indexes_created = 0
        
        with self.session() as session:
            for index in indexes:
                try:
                    result = session.run(index)
                    summary = result.consume()
                    if summary.counters.indexes_added > 0:
                        indexes_created += 1
                        logger.info(f"Created index: {index}")
                except Exception as e:
                    logger.warning(f"Index already exists or failed: {index} - {e}")
        
        return indexes_created
    
    def get_node_counts(self) -> Dict[str, int]:
        """Get count of nodes by label."""
        query = """
        MATCH (n) 
        RETURN labels(n)[0] as label, count(n) as count 
        ORDER BY count DESC
        """
        results = self.execute_query(query)
        return {record['label']: record['count'] for record in results}
    
    def get_relationship_counts(self) -> Dict[str, int]:
        """Get count of relationships by type."""
        query = """
        MATCH ()-[r]->() 
        RETURN type(r) as type, count(r) as count 
        ORDER BY count DESC
        """
        results = self.execute_query(query)
        return {record['type']: record['count'] for record in results}
    
    def close(self) -> None:
        """Close Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")


# Global client instance (initialized when needed)
_client: Optional[Neo4jClient] = None


def get_neo4j_client(config: Optional[Config] = None) -> Neo4jClient:
    """Get or create Neo4j client instance."""
    global _client
    
    if _client is None:
        from .config import config as default_config
        _client = Neo4jClient(config or default_config)
    
    return _client


def close_neo4j_client() -> None:
    """Close global Neo4j client."""
    global _client
    if _client:
        _client.close()
        _client = None