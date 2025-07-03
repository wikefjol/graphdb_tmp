#!/usr/bin/env python3
"""
Streaming Data Loader
Efficiently read and stream large ES response JSON files for incremental processing
"""

import json
import os
from typing import Dict, List, Iterator, Optional, Tuple
from collections import defaultdict
from pathlib import Path


class StreamingESDataReader:
    """Stream large Elasticsearch response files for batch processing"""
    
    def __init__(self, file_path: str):
        """Initialize streaming reader
        
        Args:
            file_path: Path to ES response JSON file
        """
        self.file_path = Path(file_path)
        self.file_size = self.file_path.stat().st_size if self.file_path.exists() else 0
        self.total_hits = None
        self._es_data_cache = None
    
    def get_metadata(self) -> Dict:
        """Get metadata about the ES response file
        
        Returns:
            Dictionary with file and data metadata
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found: {self.file_path}")
        
        print(f"📊 Analyzing ES data file: {self.file_path}")
        print(f"   File size: {self.file_size / (1024*1024):.1f} MB")
        
        # Load and cache the full ES response for metadata
        if self._es_data_cache is None:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                self._es_data_cache = json.load(f)
        
        # Extract metadata
        total_hits = self._es_data_cache.get('hits', {}).get('total', 0)
        if isinstance(total_hits, dict):
            total_hits = total_hits.get('value', 0)
        
        self.total_hits = total_hits
        actual_hits = len(self._es_data_cache.get('hits', {}).get('hits', []))
        
        metadata = {
            'file_size_mb': self.file_size / (1024*1024),
            'total_hits_reported': total_hits,
            'actual_documents': actual_hits,
            'took_ms': self._es_data_cache.get('took', 0),
            'timed_out': self._es_data_cache.get('timed_out', False),
            'scroll_id': self._es_data_cache.get('_scroll_id', 'N/A')
        }
        
        print(f"   Total hits reported: {metadata['total_hits_reported']:,}")
        print(f"   Actual documents: {metadata['actual_documents']:,}")
        print(f"   ES query took: {metadata['took_ms']}ms")
        
        return metadata
    
    def stream_publication_batches(self, 
                                 batch_size: int = 50, 
                                 start_offset: int = 0, 
                                 max_batches: Optional[int] = None) -> Iterator[Tuple[int, List[Dict]]]:
        """Stream publication documents in batches
        
        Args:
            batch_size: Number of publications per batch
            start_offset: Starting index (for resume capability)
            max_batches: Maximum number of batches to process
            
        Yields:
            Tuple of (batch_number, publication_documents)
        """
        # Ensure metadata is loaded
        if self._es_data_cache is None:
            self.get_metadata()
        
        hits = self._es_data_cache.get('hits', {}).get('hits', [])
        total_docs = len(hits)
        
        print(f"🔄 Starting batch streaming:")
        print(f"   Total documents: {total_docs:,}")
        print(f"   Batch size: {batch_size}")
        print(f"   Start offset: {start_offset}")
        print(f"   Max batches: {max_batches or 'unlimited'}")
        
        batch_number = 0
        current_offset = start_offset
        
        while current_offset < total_docs:
            # Check max batches limit
            if max_batches is not None and batch_number >= max_batches:
                print(f"   ⏹️  Reached max batches limit: {max_batches}")
                break
            
            # Extract batch
            end_offset = min(current_offset + batch_size, total_docs)
            batch_docs = hits[current_offset:end_offset]
            
            if not batch_docs:
                break
            
            batch_number += 1
            progress_pct = (end_offset / total_docs) * 100
            
            print(f"   📦 Batch {batch_number}: docs {current_offset}-{end_offset-1} ({progress_pct:.1f}%)")
            
            yield batch_number, batch_docs
            
            current_offset = end_offset
        
        print(f"✅ Streaming complete: {batch_number} batches processed")
    
    def extract_nested_entities_from_publications(self, publications: List[Dict]) -> Dict[str, List[Dict]]:
        """Extract nested entities (persons, organizations) from publication documents
        
        Args:
            publications: List of ES publication documents
            
        Returns:
            Dict with keys: 'persons', 'organizations', 'publication_ids'
        """
        persons = {}  # Use dict to deduplicate by ID
        organizations = {}  # Use dict to deduplicate by ID
        publication_ids = []
        
        for pub_doc in publications:
            # Extract publication ID
            pub_id = pub_doc.get('_id')
            if pub_id:
                publication_ids.append(pub_id)
            
            # Extract publication source
            pub_source = pub_doc.get('_source', {})
            
            # Extract persons from Persons array
            persons_array = pub_source.get('Persons', [])
            for person_data in persons_array:
                if isinstance(person_data, dict):
                    person_ref = person_data.get('PersonData', {})
                    person_id = person_ref.get('Id')
                    
                    if person_id and person_id not in persons:
                        # Add ES _id for consistency
                        person_ref['_es_id'] = person_id
                        persons[person_id] = person_ref
                    
                    # Extract organizations from this person's affiliations
                    person_orgs = person_data.get('Organizations', [])
                    for org_data in person_orgs:
                        if isinstance(org_data, dict):
                            org_ref = org_data.get('OrganizationData', {})
                            org_id = org_ref.get('Id')
                            
                            if org_id and org_id not in organizations:
                                # Add ES _id for consistency
                                org_ref['_es_id'] = org_id
                                organizations[org_id] = org_ref
        
        # Convert dicts back to lists
        entity_summary = {
            'persons': list(persons.values()),
            'organizations': list(organizations.values()),
            'publication_ids': publication_ids
        }
        
        print(f"   🔍 Extracted from {len(publications)} publications:")
        print(f"     👥 Persons: {len(entity_summary['persons'])}")
        print(f"     🏢 Organizations: {len(entity_summary['organizations'])}")
        print(f"     📚 Publications: {len(entity_summary['publication_ids'])}")
        
        return entity_summary
    
    def get_entity_statistics(self, max_analyze: int = 100) -> Dict:
        """Analyze entity distribution in the first N publications for planning
        
        Args:
            max_analyze: Maximum publications to analyze for stats
            
        Returns:
            Statistics about entity counts and relationships
        """
        # Ensure metadata is loaded
        if self._es_data_cache is None:
            self.get_metadata()
        
        hits = self._es_data_cache.get('hits', {}).get('hits', [])
        analyze_count = min(max_analyze, len(hits))
        
        print(f"📈 Analyzing entity distribution (first {analyze_count} publications):")
        
        total_persons = set()
        total_organizations = set()
        publications_with_persons = 0
        publications_with_orgs = 0
        person_count_dist = defaultdict(int)
        org_count_dist = defaultdict(int)
        
        for i, pub_doc in enumerate(hits[:analyze_count]):
            pub_source = pub_doc.get('_source', {})
            
            # Count persons
            persons_array = pub_source.get('Persons', [])
            pub_person_count = 0
            pub_org_count = 0
            
            for person_data in persons_array:
                if isinstance(person_data, dict):
                    person_ref = person_data.get('PersonData', {})
                    person_id = person_ref.get('Id')
                    if person_id:
                        total_persons.add(person_id)
                        pub_person_count += 1
                    
                    # Count orgs from this person
                    person_orgs = person_data.get('Organizations', [])
                    for org_data in person_orgs:
                        if isinstance(org_data, dict):
                            org_ref = org_data.get('OrganizationData', {})
                            org_id = org_ref.get('Id')
                            if org_id:
                                total_organizations.add(org_id)
                                pub_org_count += 1
            
            if pub_person_count > 0:
                publications_with_persons += 1
                person_count_dist[pub_person_count] += 1
            
            if pub_org_count > 0:
                publications_with_orgs += 1
                org_count_dist[pub_org_count] += 1
        
        # Calculate averages
        avg_persons = len(total_persons) / analyze_count if analyze_count > 0 else 0
        avg_orgs = len(total_organizations) / analyze_count if analyze_count > 0 else 0
        
        stats = {
            'analyzed_publications': analyze_count,
            'unique_persons': len(total_persons),
            'unique_organizations': len(total_organizations),
            'publications_with_persons': publications_with_persons,
            'publications_with_orgs': publications_with_orgs,
            'avg_persons_per_pub': avg_persons,
            'avg_orgs_per_pub': avg_orgs,
            'person_count_distribution': dict(person_count_dist),
            'org_count_distribution': dict(org_count_dist)
        }
        
        print(f"   📊 Results:")
        print(f"     Unique persons: {stats['unique_persons']:,}")
        print(f"     Unique organizations: {stats['unique_organizations']:,}")
        print(f"     Pubs with persons: {stats['publications_with_persons']}/{analyze_count}")
        print(f"     Pubs with orgs: {stats['publications_with_orgs']}/{analyze_count}")
        print(f"     Avg persons/pub: {stats['avg_persons_per_pub']:.1f}")
        print(f"     Avg orgs/pub: {stats['avg_orgs_per_pub']:.1f}")
        
        return stats


class EntityBatchProcessor:
    """Process batches of entities with deduplication and validation"""
    
    def __init__(self):
        """Initialize batch processor"""
        self.processed_entities = {
            'persons': set(),
            'organizations': set(),
            'publications': set()
        }
    
    def filter_new_entities(self, entity_type: str, entities: List[Dict]) -> List[Dict]:
        """Filter entities to only include new ones not yet processed
        
        Args:
            entity_type: Type of entity ('persons', 'organizations', 'publications')
            entities: List of entity dictionaries
            
        Returns:
            List of new entities not yet processed
        """
        if entity_type not in self.processed_entities:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        new_entities = []
        processed_set = self.processed_entities[entity_type]
        
        for entity in entities:
            entity_id = entity.get('_es_id') or entity.get('Id') or entity.get('id')
            if entity_id and entity_id not in processed_set:
                new_entities.append(entity)
                processed_set.add(entity_id)
        
        return new_entities
    
    def get_processing_stats(self) -> Dict[str, int]:
        """Get statistics about processed entities
        
        Returns:
            Dictionary with counts of processed entities
        """
        return {
            'processed_persons': len(self.processed_entities['persons']),
            'processed_organizations': len(self.processed_entities['organizations']),
            'processed_publications': len(self.processed_entities['publications'])
        }


def get_streaming_reader(file_path: str) -> StreamingESDataReader:
    """Get streaming ES data reader instance
    
    Args:
        file_path: Path to ES response JSON file
        
    Returns:
        StreamingESDataReader instance
    """
    return StreamingESDataReader(file_path)


def get_batch_processor() -> EntityBatchProcessor:
    """Get entity batch processor instance
    
    Returns:
        EntityBatchProcessor instance
    """
    return EntityBatchProcessor()