#!/usr/bin/env python3
"""
Incremental Data Loader
Load research data in batches with progress tracking and resumable processing
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from pathlib import Path

from ..core.config import Config
from ..core.neo4j_client import Neo4jClient, get_neo4j_client
from ..transformers.es_transformer import ESTransformer, ESRelationshipExtractor
from ..loaders.streaming_loader import StreamingESDataReader
from ..models.person import PersonCreate
from ..models.publication import PublicationCreate 
from ..models.organization import OrganizationCreate


class EntityTracker:
    """Track entities that have been loaded to prevent duplicates"""
    
    def __init__(self, neo4j_client: Neo4jClient):
        """Initialize entity tracker
        
        Args:
            neo4j_client: Neo4j client instance
        """
        self.client = neo4j_client
        self._existing_entities = {
            'Person': set(),
            'Publication': set(),
            'Organization': set()
        }
        self._loaded = False
    
    def load_existing_entities(self):
        """Load all existing entity IDs from Neo4j"""
        print("🔍 Loading existing entities from Neo4j...")
        
        for entity_type in self._existing_entities.keys():
            query = f"MATCH (n:{entity_type}) RETURN n.id as id"
            result = self.client.execute_query(query)
            existing_ids = {record['id'] for record in result if record['id']}
            self._existing_entities[entity_type] = existing_ids
            print(f"   {entity_type}: {len(existing_ids)} existing")
        
        self._loaded = True
    
    def is_entity_existing(self, entity_type: str, entity_id: str) -> bool:
        """Check if entity already exists
        
        Args:
            entity_type: Type of entity
            entity_id: Entity ID to check
            
        Returns:
            True if entity exists
        """
        if not self._loaded:
            self.load_existing_entities()
        return entity_id in self._existing_entities.get(entity_type, set())
    
    def mark_entity_loaded(self, entity_type: str, entity_id: str):
        """Mark entity as loaded
        
        Args:
            entity_type: Type of entity
            entity_id: Entity ID to mark as loaded
        """
        if not self._loaded:
            self.load_existing_entities()
        self._existing_entities[entity_type].add(entity_id)
    
    def get_new_entities(self, entity_type: str, entities: List[Dict]) -> List[Dict]:
        """Filter entities to only include new ones
        
        Args:
            entity_type: Type of entity
            entities: List of entity dictionaries
            
        Returns:
            List of new entities
        """
        if not self._loaded:
            self.load_existing_entities()
        
        new_entities = []
        for entity in entities:
            entity_id = entity.get('id')
            if entity_id and not self.is_entity_existing(entity_type, entity_id):
                new_entities.append(entity)
        
        return new_entities


class IncrementalDataLoader:
    """Main incremental data loading orchestrator"""
    
    def __init__(self, data_file_path: str, progress_file: str = "ingestion_progress.json"):
        """Initialize incremental data loader
        
        Args:
            data_file_path: Path to ES data file
            progress_file: Path to progress checkpoint file
        """
        # Load configuration
        self.config = Config()
        
        # Neo4j client
        self.neo4j_client = get_neo4j_client(self.config)
        
        # Components
        self.data_reader = StreamingESDataReader(data_file_path)
        self.transformer = ESTransformer()
        self.rel_extractor = ESRelationshipExtractor()
        self.entity_tracker = EntityTracker(self.neo4j_client)
        
        # Progress tracking
        self.progress_file = Path(progress_file)
        self.progress = self.load_progress()
        
        # Statistics
        self.stats = {
            'batches_processed': 0,
            'total_publications_processed': 0,
            'total_nodes_created': 0,
            'total_relationships_created': 0,
            'start_time': None,
            'last_checkpoint': None
        }
    
    def close(self):
        """Close connections"""
        self.neo4j_client.close()
    
    def load_progress(self) -> Dict:
        """Load progress from checkpoint file
        
        Returns:
            Progress dictionary
        """
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    print(f"📄 Loaded progress: {progress.get('last_processed_offset', 0)} documents processed")
                    return progress
            except Exception as e:
                print(f"⚠️  Failed to load progress file: {e}")
        
        return {
            'last_processed_offset': 0,
            'total_processed': 0,
            'batches_completed': 0,
            'created_at': datetime.now().isoformat()
        }
    
    def save_progress(self):
        """Save current progress to checkpoint file"""
        self.progress['last_updated'] = datetime.now().isoformat()
        self.progress['stats'] = self.stats
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
            print(f"💾 Progress saved: {self.progress['total_processed']} total processed")
        except Exception as e:
            print(f"❌ Failed to save progress: {e}")
    
    def process_publication_batch(self, publications: List[Dict]) -> Dict:
        """Process a batch of publications and return statistics
        
        Args:
            publications: List of ES publication documents
            
        Returns:
            Dict with processing statistics
        """
        batch_stats = {
            'publications_in_batch': len(publications),
            'new_nodes_created': 0,
            'new_relationships_created': 0,
            'skipped_duplicates': 0
        }
        
        # Extract nested entities
        nested_entities = self.data_reader.extract_nested_entities_from_publications(publications)
        
        # Transform publications
        pub_nodes = []
        for pub_doc in publications:
            try:
                # Add _es_id from document _id
                pub_source = pub_doc.get('_source', {})
                pub_source['_es_id'] = pub_doc.get('_id')
                
                transformed = self.transformer.transform_publication(pub_source)
                pub_nodes.append(transformed.dict())
            except Exception as e:
                print(f"   ⚠️  Failed to transform publication {pub_doc.get('_id')}: {e}")
        
        # Transform persons
        person_nodes = []
        for person_data in nested_entities['persons']:
            try:
                transformed = self.transformer.transform_person(person_data)
                person_nodes.append(transformed.dict())
            except Exception as e:
                print(f"   ⚠️  Failed to transform person {person_data.get('Id')}: {e}")
        
        # Transform organizations
        org_nodes = []
        for org_data in nested_entities['organizations']:
            try:
                transformed = self.transformer.transform_organization(org_data)
                org_nodes.append(transformed.dict())
            except Exception as e:
                print(f"   ⚠️  Failed to transform organization {org_data.get('Id')}: {e}")
        
        # Filter out existing entities
        new_pubs = self.entity_tracker.get_new_entities('Publication', pub_nodes)
        new_persons = self.entity_tracker.get_new_entities('Person', person_nodes)
        new_orgs = self.entity_tracker.get_new_entities('Organization', org_nodes)
        
        batch_stats['skipped_duplicates'] = (
            len(pub_nodes) - len(new_pubs) +
            len(person_nodes) - len(new_persons) +
            len(org_nodes) - len(new_orgs)
        )
        
        print(f"   📊 Batch filtering: {len(new_pubs)} new pubs, {len(new_persons)} new persons, {len(new_orgs)} new orgs")
        print(f"   🔄 Skipped {batch_stats['skipped_duplicates']} duplicates")
        
        # Load nodes to Neo4j
        nodes_created = 0
        nodes_created += self.load_nodes_batch('Publication', new_pubs)
        nodes_created += self.load_nodes_batch('Person', new_persons)
        nodes_created += self.load_nodes_batch('Organization', new_orgs)
        
        batch_stats['new_nodes_created'] = nodes_created
        
        # Update entity tracker
        for pub in new_pubs:
            self.entity_tracker.mark_entity_loaded('Publication', pub['id'])
        for person in new_persons:
            self.entity_tracker.mark_entity_loaded('Person', person['id'])
        for org in new_orgs:
            self.entity_tracker.mark_entity_loaded('Organization', org['id'])
        
        # Create relationships
        relationships_created = self.create_batch_relationships(publications, nested_entities)
        batch_stats['new_relationships_created'] = relationships_created
        
        return batch_stats
    
    def load_nodes_batch(self, node_type: str, nodes: List[Dict]) -> int:
        """Load a batch of nodes to Neo4j
        
        Args:
            node_type: Type of nodes to create
            nodes: List of node dictionaries
            
        Returns:
            Number of nodes created
        """
        if not nodes:
            return 0
        
        # Convert datetime objects to ISO strings
        processed_nodes = []
        for node in nodes:
            processed_node = {}
            for key, value in node.items():
                if isinstance(value, datetime):
                    processed_node[key] = value.isoformat()
                else:
                    processed_node[key] = value
            processed_nodes.append(processed_node)
        
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:{node_type} {{id: node.id}})
        SET n = node,
            n.createdAt = CASE WHEN n.createdAt IS NULL THEN datetime() ELSE n.createdAt END,
            n.updatedAt = datetime()
        """
        
        try:
            result = self.neo4j_client.execute_write(query, {'nodes': processed_nodes})
            nodes_created = result.get('nodes_created', 0)
            print(f"   ✅ {node_type}: {nodes_created} nodes created")
            return nodes_created
        except Exception as e:
            print(f"   ❌ {node_type}: {str(e)}")
            return 0
    
    def create_batch_relationships(self, publications: List[Dict], nested_entities: Dict = None) -> int:
        """Create relationships for a batch of publications and entities
        
        Args:
            publications: List of ES publication documents
            nested_entities: Nested entities dictionary
            
        Returns:
            Number of relationships created
        """
        total_rels_created = 0
        
        # Extract authorship relationships
        authorship_rels = self.rel_extractor.extract_authorship_relationships(publications)
        if authorship_rels:
            query = """
            UNWIND $relationships AS rel
            MATCH (p:Person {id: rel.sourceId})
            MATCH (pub:Publication {id: rel.targetId})
            MERGE (p)-[r:AUTHORED]->(pub)
            ON CREATE SET 
                r.order = rel.order,
                r.role = rel.role,
                r.createdAt = datetime()
            ON MATCH SET
                r.updatedAt = datetime(),
                r.order = CASE WHEN r.order IS NULL THEN rel.order ELSE r.order END
            """
            
            try:
                result = self.neo4j_client.execute_write(query, {'relationships': authorship_rels})
                rels_created = result.get('relationships_created', 0)
                total_rels_created += rels_created
                print(f"   ✅ AUTHORED: {rels_created} relationships")
            except Exception as e:
                print(f"   ❌ AUTHORED: {str(e)}")
        
        # Extract affiliation relationships from persons in publications
        all_persons = []
        for pub in publications:
            pub_source = pub.get('_source', {})
            persons_array = pub_source.get('Persons', [])
            for person_data in persons_array:
                if isinstance(person_data, dict):
                    person_ref = person_data.get('PersonData', {})
                    if person_ref.get('Id'):
                        # Add _es_id for consistency
                        person_ref['_es_id'] = person_ref.get('Id')
                        all_persons.append({'_source': person_ref})
        
        affiliation_rels = self.rel_extractor.extract_affiliation_relationships(all_persons)
        if affiliation_rels:
            query = """
            UNWIND $relationships AS rel
            MATCH (p:Person {id: rel.sourceId})
            MATCH (o:Organization {id: rel.targetId})
            MERGE (p)-[r:AFFILIATED_WITH]->(o)
            ON CREATE SET 
                r.startDate = CASE 
                    WHEN rel.startDate IS NOT NULL 
                    THEN date(substring(rel.startDate, 0, 10)) 
                    ELSE NULL 
                END,
                r.endDate = CASE 
                    WHEN rel.endDate IS NOT NULL 
                    THEN date(substring(rel.endDate, 0, 10)) 
                    ELSE NULL 
                END,
                r.title = rel.title,
                r.priority = rel.priority,
                r.createdAt = datetime()
            ON MATCH SET
                r.updatedAt = datetime(),
                r.endDate = CASE 
                    WHEN rel.endDate IS NOT NULL 
                    THEN date(substring(rel.endDate, 0, 10)) 
                    ELSE r.endDate 
                END,
                r.title = CASE WHEN rel.title IS NOT NULL THEN rel.title ELSE r.title END,
                r.priority = CASE WHEN rel.priority IS NOT NULL THEN rel.priority ELSE r.priority END
            """
            
            try:
                result = self.neo4j_client.execute_write(query, {'relationships': affiliation_rels})
                rels_created = result.get('relationships_created', 0)
                total_rels_created += rels_created
                print(f"   ✅ AFFILIATED_WITH: {rels_created} relationships")
            except Exception as e:
                print(f"   ❌ AFFILIATED_WITH: {str(e)}")
        
        return total_rels_created
    
    def run_incremental_loading(self, 
                               batch_size: int = 50, 
                               max_batches: Optional[int] = None,
                               checkpoint_every: int = 5) -> Dict:
        """Run the incremental loading process
        
        Args:
            batch_size: Number of publications per batch
            max_batches: Maximum batches to process (None for all)
            checkpoint_every: Save progress every N batches
            
        Returns:
            Final statistics
        """
        print("🚀 INCREMENTAL DATA LOADER")
        print("=" * 80)
        
        # Initialize
        self.stats['start_time'] = datetime.now().isoformat()
        metadata = self.data_reader.get_metadata()
        
        start_offset = self.progress.get('last_processed_offset', 0)
        print(f"   Starting from offset: {start_offset}")
        print(f"   Batch size: {batch_size}")
        print(f"   Max batches: {max_batches or 'unlimited'}")
        print(f"   Checkpoint every: {checkpoint_every} batches")
        
        # Load existing entities
        self.entity_tracker.load_existing_entities()
        
        # Process batches
        try:
            for batch_num, batch_docs in self.data_reader.stream_publication_batches(
                batch_size=batch_size, 
                start_offset=start_offset, 
                max_batches=max_batches
            ):
                print(f"\n📦 Processing batch {batch_num}...")
                
                # Process this batch
                batch_stats = self.process_publication_batch(batch_docs)
                
                # Update overall statistics
                self.stats['batches_processed'] += 1
                self.stats['total_publications_processed'] += batch_stats['publications_in_batch']
                self.stats['total_nodes_created'] += batch_stats['new_nodes_created']
                self.stats['total_relationships_created'] += batch_stats['new_relationships_created']
                
                # Update progress
                self.progress['last_processed_offset'] = start_offset + (batch_num * batch_size)
                self.progress['total_processed'] += batch_stats['publications_in_batch']
                self.progress['batches_completed'] += 1
                
                # Checkpoint progress
                if batch_num % checkpoint_every == 0:
                    self.stats['last_checkpoint'] = datetime.now().isoformat()
                    self.save_progress()
                
                # Show progress
                total_docs = metadata['actual_documents']
                progress_pct = (self.progress['total_processed'] / total_docs) * 100
                print(f"   📊 Batch {batch_num} complete:")
                print(f"     Publications: {batch_stats['publications_in_batch']}")
                print(f"     New nodes: {batch_stats['new_nodes_created']}")
                print(f"     New relationships: {batch_stats['new_relationships_created']}")
                print(f"     Skipped duplicates: {batch_stats['skipped_duplicates']}")
                print(f"     Overall progress: {self.progress['total_processed']}/{total_docs} ({progress_pct:.1f}%)")
        
        except KeyboardInterrupt:
            print(f"\n⏸️  Interrupted by user")
            self.save_progress()
        except Exception as e:
            print(f"\n❌ Error during processing: {e}")
            self.save_progress()
            raise
        
        # Final save
        self.save_progress()
        
        # Summary
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.stats['start_time'])
        duration = end_time - start_time
        
        print(f"\n" + "=" * 80)
        print("✅ INCREMENTAL LOADING COMPLETE")
        print("=" * 80)
        print(f"📊 Final Statistics:")
        print(f"   Batches processed: {self.stats['batches_processed']}")
        print(f"   Publications processed: {self.stats['total_publications_processed']}")
        print(f"   Total nodes created: {self.stats['total_nodes_created']}")
        print(f"   Total relationships created: {self.stats['total_relationships_created']}")
        print(f"   Duration: {duration}")
        if duration.total_seconds() > 0:
            print(f"   Average: {self.stats['total_publications_processed'] / duration.total_seconds():.1f} pubs/sec")
        
        return self.stats


def get_incremental_loader(data_file_path: str, progress_file: str = "ingestion_progress.json") -> IncrementalDataLoader:
    """Get incremental data loader instance
    
    Args:
        data_file_path: Path to ES data file
        progress_file: Path to progress checkpoint file
        
    Returns:
        IncrementalDataLoader instance
    """
    return IncrementalDataLoader(data_file_path, progress_file)