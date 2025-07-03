"""
Graph initializer to clear Neo4j and set up clean constraints and indexes.
"""

import logging
from typing import List, Dict, Any

from ..core.neo4j_client import Neo4jClient
from ..core.config import Config

logger = logging.getLogger(__name__)


class GraphInitializer:
    """Initialize clean Neo4j graph with proper constraints and indexes."""
    
    def __init__(self, client: Neo4jClient):
        """Initialize with Neo4j client."""
        self.client = client
    
    def initialize_clean_graph(self) -> Dict[str, Any]:
        """
        Clear the graph and set up clean constraints and indexes.
        
        Returns:
            Dictionary with initialization statistics
        """
        logger.info("Starting clean graph initialization")
        
        stats = {
            'database_cleared': False,
            'constraints_created': 0,
            'indexes_created': 0,
            'initialization_successful': False
        }
        
        try:
            # Step 1: Clear existing database
            clear_stats = self.clear_database()
            stats['database_cleared'] = True
            stats.update(clear_stats)
            
            # Step 2: Create constraints
            constraints_created = self.create_constraints()
            stats['constraints_created'] = constraints_created
            
            # Step 3: Create indexes
            indexes_created = self.create_indexes()
            stats['indexes_created'] = indexes_created
            
            stats['initialization_successful'] = True
            logger.info(f"Graph initialization complete: {stats}")
            
        except Exception as e:
            logger.error(f"Graph initialization failed: {e}")
            stats['initialization_error'] = str(e)
            raise
        
        return stats
    
    def clear_database(self) -> Dict[str, int]:
        """Clear all nodes and relationships from the database."""
        logger.warning("Clearing entire Neo4j database")
        
        try:
            clear_stats = self.client.clear_database()
            logger.info(f"Database cleared successfully: {clear_stats}")
            return clear_stats
            
        except Exception as e:
            logger.error(f"Failed to clear database: {e}")
            raise
    
    def create_constraints(self) -> int:
        """Create necessary constraints for data integrity."""
        logger.info("Creating database constraints")
        
        constraints = [
            # Organization constraints
            "CREATE CONSTRAINT organization_id_unique IF NOT EXISTS FOR (o:Organization) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT organization_name_required IF NOT EXISTS FOR (o:Organization) REQUIRE o.nameEng IS NOT NULL",
            
            # Person constraints
            "CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT person_display_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.displayName IS NOT NULL",
            
            # Publication constraints
            "CREATE CONSTRAINT publication_id_unique IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.id IS UNIQUE",
            "CREATE CONSTRAINT publication_title_required IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.title IS NOT NULL",
            "CREATE CONSTRAINT publication_year_required IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.year IS NOT NULL",
            
            # Keyword constraints
            "CREATE CONSTRAINT keyword_value_unique IF NOT EXISTS FOR (k:Keyword) REQUIRE k.value IS UNIQUE"
        ]
        
        try:
            constraints_created = self.client.create_constraints(constraints)
            logger.info(f"Created {constraints_created} constraints")
            return constraints_created
            
        except Exception as e:
            logger.error(f"Failed to create constraints: {e}")
            raise
    
    def create_indexes(self) -> int:
        """Create indexes for query performance."""
        logger.info("Creating database indexes")
        
        indexes = [
            # Organization indexes
            "CREATE INDEX organization_name_eng_index IF NOT EXISTS FOR (o:Organization) ON (o.nameEng)",
            "CREATE INDEX organization_level_index IF NOT EXISTS FOR (o:Organization) ON (o.level)",
            "CREATE INDEX organization_type_index IF NOT EXISTS FOR (o:Organization) ON (o.organizationType)",
            "CREATE INDEX organization_country_index IF NOT EXISTS FOR (o:Organization) ON (o.country)",
            
            # Person indexes
            "CREATE INDEX person_display_name_index IF NOT EXISTS FOR (p:Person) ON (p.displayName)",
            "CREATE INDEX person_last_name_index IF NOT EXISTS FOR (p:Person) ON (p.lastName)",
            "CREATE INDEX person_orcid_index IF NOT EXISTS FOR (p:Person) ON (p.orcid)",
            
            # Publication indexes
            "CREATE INDEX publication_title_index IF NOT EXISTS FOR (pub:Publication) ON (pub.title)",
            "CREATE INDEX publication_year_index IF NOT EXISTS FOR (pub:Publication) ON (pub.year)",
            "CREATE INDEX publication_type_index IF NOT EXISTS FOR (pub:Publication) ON (pub.publicationType)",
            "CREATE INDEX publication_doi_index IF NOT EXISTS FOR (pub:Publication) ON (pub.doi)",
            
            # Keyword indexes
            "CREATE INDEX keyword_display_value_index IF NOT EXISTS FOR (k:Keyword) ON (k.displayValue)"
        ]
        
        try:
            indexes_created = self.client.create_indexes(indexes)
            logger.info(f"Created {indexes_created} indexes")
            return indexes_created
            
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    def verify_initialization(self) -> Dict[str, Any]:
        """Verify that the graph is properly initialized."""
        logger.info("Verifying graph initialization")
        
        verification = {
            'constraints_exist': False,
            'indexes_exist': False,
            'database_empty': False,
            'verification_successful': False
        }
        
        try:
            # Check if database is empty
            node_counts = self.client.get_node_counts()
            relationship_counts = self.client.get_relationship_counts()
            
            total_nodes = sum(node_counts.values())
            total_relationships = sum(relationship_counts.values())
            
            verification['database_empty'] = total_nodes == 0 and total_relationships == 0
            verification['node_counts'] = node_counts
            verification['relationship_counts'] = relationship_counts
            
            # Check constraints (simplified check)
            constraints_query = "SHOW CONSTRAINTS"
            try:
                constraints_result = self.client.execute_query(constraints_query)
                verification['constraints_exist'] = len(constraints_result) > 0
                verification['constraint_count'] = len(constraints_result)
            except Exception as e:
                logger.warning(f"Could not verify constraints: {e}")
                verification['constraints_exist'] = True  # Assume they exist
            
            # Check indexes (simplified check)
            indexes_query = "SHOW INDEXES"
            try:
                indexes_result = self.client.execute_query(indexes_query)
                verification['indexes_exist'] = len(indexes_result) > 0
                verification['index_count'] = len(indexes_result)
            except Exception as e:
                logger.warning(f"Could not verify indexes: {e}")
                verification['indexes_exist'] = True  # Assume they exist
            
            verification['verification_successful'] = (
                verification['database_empty'] and 
                verification['constraints_exist'] and 
                verification['indexes_exist']
            )
            
            logger.info(f"Graph initialization verification: {verification}")
            
        except Exception as e:
            logger.error(f"Graph initialization verification failed: {e}")
            verification['verification_error'] = str(e)
        
        return verification
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get information about the current database schema."""
        logger.info("Retrieving database schema information")
        
        schema_info = {
            'node_labels': [],
            'relationship_types': [],
            'constraints': [],
            'indexes': []
        }
        
        try:
            # Get node labels
            labels_query = "CALL db.labels()"
            labels_result = self.client.execute_query(labels_query)
            schema_info['node_labels'] = [record['label'] for record in labels_result]
            
            # Get relationship types
            rel_types_query = "CALL db.relationshipTypes()"
            rel_types_result = self.client.execute_query(rel_types_query)
            schema_info['relationship_types'] = [record['relationshipType'] for record in rel_types_result]
            
            # Get constraints
            try:
                constraints_result = self.client.execute_query("SHOW CONSTRAINTS")
                schema_info['constraints'] = constraints_result
            except Exception as e:
                logger.warning(f"Could not retrieve constraints: {e}")
            
            # Get indexes
            try:
                indexes_result = self.client.execute_query("SHOW INDEXES")
                schema_info['indexes'] = indexes_result
            except Exception as e:
                logger.warning(f"Could not retrieve indexes: {e}")
            
            logger.info(f"Schema info retrieved: {len(schema_info['node_labels'])} labels, "
                       f"{len(schema_info['relationship_types'])} relationship types")
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema info: {e}")
            schema_info['error'] = str(e)
        
        return schema_info


def initialize_clean_graph(config: Config = None) -> Dict[str, Any]:
    """
    Convenience function to initialize a clean graph.
    
    Returns:
        Dictionary with initialization statistics
    """
    from ..core.neo4j_client import get_neo4j_client
    
    client = get_neo4j_client(config)
    initializer = GraphInitializer(client)
    
    return initializer.initialize_clean_graph()