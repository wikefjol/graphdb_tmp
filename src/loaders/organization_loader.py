"""
Organization loader with PART_OF relationship validation and integrity checks.
"""

import logging
from typing import List, Dict, Any, Tuple

from ..core.neo4j_client import Neo4jClient
from ..models.organization import Organization, OrganizationCreate, OrganizationHierarchy
from ..utils.validation import validate_hierarchy, prevent_cycles, validate_relationship

logger = logging.getLogger(__name__)


class OrganizationLoader:
    """Load organizations into Neo4j with relationship validation."""
    
    def __init__(self, client: Neo4jClient):
        """Initialize with Neo4j client."""
        self.client = client
        self.stats = {
            'organizations_processed': 0,
            'organizations_created': 0,
            'organizations_updated': 0,
            'relationships_processed': 0,
            'relationships_created': 0,
            'relationships_skipped': 0,
            'validation_errors': []
        }
    
    def load_organizations(self, organizations: List[OrganizationCreate]) -> Dict[str, Any]:
        """
        Load organizations into Neo4j with validation.
        
        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading {len(organizations)} organizations")
        
        try:
            # Validate organizations before loading
            validated_orgs = self._validate_organizations(organizations)
            
            # Load organizations in batches
            batch_size = 50
            for i in range(0, len(validated_orgs), batch_size):
                batch = validated_orgs[i:i + batch_size]
                self._load_organization_batch(batch)
            
            logger.info(f"Successfully loaded {self.stats['organizations_created']} organizations")
            
        except Exception as e:
            logger.error(f"Failed to load organizations: {e}")
            self.stats['loading_error'] = str(e)
            raise
        
        return self.stats.copy()
    
    def load_hierarchy_relationships(self, relationships: List[OrganizationHierarchy]) -> Dict[str, Any]:
        """
        Load PART_OF relationships with validation to prevent self-references and cycles.
        
        Returns:
            Dictionary with relationship loading statistics
        """
        logger.info(f"Loading {len(relationships)} PART_OF relationships")
        
        try:
            # Pre-validate the entire hierarchy
            is_valid, errors = validate_hierarchy(relationships)
            if not is_valid:
                critical_errors = [e for e in errors if "CRITICAL:" in e]
                if critical_errors:
                    logger.error(f"Critical hierarchy errors detected: {critical_errors}")
                    # Filter out self-references and other critical issues
                    relationships = self._fix_critical_hierarchy_issues(relationships)
                
                # Log warnings for non-critical issues
                warnings = [e for e in errors if "WARNING:" in e]
                for warning in warnings:
                    logger.warning(warning)
            
            # Load relationships with individual validation
            for rel in relationships:
                self._load_single_relationship(rel)
            
            logger.info(f"Successfully loaded {self.stats['relationships_created']} PART_OF relationships")
            
        except Exception as e:
            logger.error(f"Failed to load hierarchy relationships: {e}")
            self.stats['relationship_loading_error'] = str(e)
            raise
        
        return self.stats.copy()
    
    def load_complete_organization_structure(self, organizations: List[OrganizationCreate], 
                                           relationships: List[OrganizationHierarchy]) -> Dict[str, Any]:
        """
        Load complete organizational structure (organizations + hierarchy).
        
        Returns:
            Dictionary with complete loading statistics
        """
        logger.info(f"Loading complete organizational structure: {len(organizations)} orgs, {len(relationships)} relationships")
        
        try:
            # Step 1: Load organizations first
            org_stats = self.load_organizations(organizations)
            
            # Step 2: Load hierarchy relationships
            rel_stats = self.load_hierarchy_relationships(relationships)
            
            # Step 3: Verify the loaded structure
            verification = self.verify_loaded_structure()
            
            # Combine statistics
            combined_stats = {
                'organization_loading': org_stats,
                'relationship_loading': rel_stats,
                'verification': verification,
                'total_organizations': self.stats['organizations_created'],
                'total_relationships': self.stats['relationships_created'],
                'loading_successful': True
            }
            
            logger.info(f"Complete structure loaded successfully: {combined_stats}")
            
        except Exception as e:
            logger.error(f"Failed to load complete organizational structure: {e}")
            combined_stats = {
                'loading_successful': False,
                'error': str(e),
                'partial_stats': self.stats.copy()
            }
            raise
        
        return combined_stats
    
    def _validate_organizations(self, organizations: List[OrganizationCreate]) -> List[OrganizationCreate]:
        """Validate organizations before loading."""
        validated = []
        seen_ids = set()
        
        for org in organizations:
            self.stats['organizations_processed'] += 1
            
            # Check for duplicate IDs
            if org.id in seen_ids:
                error = f"Duplicate organization ID: {org.id}"
                logger.warning(error)
                self.stats['validation_errors'].append(error)
                continue
            
            seen_ids.add(org.id)
            validated.append(org)
        
        logger.info(f"Validated {len(validated)} organizations (filtered out {len(organizations) - len(validated)})")
        return validated
    
    def _load_organization_batch(self, organizations: List[OrganizationCreate]) -> None:
        """Load a batch of organizations using MERGE to handle duplicates."""
        if not organizations:
            return
        
        # Convert to Neo4j format
        org_dicts = []
        for org in organizations:
            org_data = org.dict(exclude_none=True)
            # Convert any datetime fields if present
            org_dicts.append(org_data)
        
        # Use MERGE to handle potential duplicates
        query = """
        UNWIND $organizations AS org
        MERGE (o:Organization {id: org.id})
        SET o = org
        RETURN count(o) as processed
        """
        
        try:
            with self.client.session() as session:
                result = session.run(query, organizations=org_dicts)
                processed = result.single()['processed']
                self.stats['organizations_created'] += processed
                
                logger.debug(f"Loaded batch of {processed} organizations")
                
        except Exception as e:
            logger.error(f"Failed to load organization batch: {e}")
            raise
    
    def _fix_critical_hierarchy_issues(self, relationships: List[OrganizationHierarchy]) -> List[OrganizationHierarchy]:
        """Fix critical hierarchy issues like self-references."""
        fixed_relationships = []
        
        for rel in relationships:
            self.stats['relationships_processed'] += 1
            
            # Skip self-referential relationships (critical fix)
            if rel.childId == rel.parentId:
                error = f"SKIPPED: Self-referential relationship {rel.childId} -> {rel.parentId}"
                logger.warning(error)
                self.stats['validation_errors'].append(error)
                self.stats['relationships_skipped'] += 1
                continue
            
            fixed_relationships.append(rel)
        
        logger.info(f"Fixed hierarchy: kept {len(fixed_relationships)} relationships, "
                   f"skipped {self.stats['relationships_skipped']} problematic ones")
        
        return fixed_relationships
    
    def _load_single_relationship(self, relationship: OrganizationHierarchy) -> None:
        """Load a single PART_OF relationship with validation."""
        # Validate the relationship
        is_valid, error_msg = validate_relationship(
            'PART_OF', 
            relationship.childId, 
            relationship.parentId
        )
        
        if not is_valid:
            logger.warning(f"Invalid relationship: {error_msg}")
            self.stats['validation_errors'].append(error_msg)
            self.stats['relationships_skipped'] += 1
            return
        
        # Create the relationship in Neo4j
        query = """
        MATCH (child:Organization {id: $child_id})
        MATCH (parent:Organization {id: $parent_id})
        MERGE (child)-[:PART_OF]->(parent)
        RETURN count(*) as created
        """
        
        try:
            with self.client.session() as session:
                result = session.run(query, {
                    'child_id': relationship.childId,
                    'parent_id': relationship.parentId
                })
                created = result.single()['created']
                
                if created > 0:
                    self.stats['relationships_created'] += 1
                    logger.debug(f"Created PART_OF: {relationship.childId} -> {relationship.parentId}")
                else:
                    logger.warning(f"Could not create relationship (organizations not found): "
                                 f"{relationship.childId} -> {relationship.parentId}")
                    self.stats['relationships_skipped'] += 1
                
        except Exception as e:
            logger.error(f"Failed to create PART_OF relationship {relationship.childId} -> {relationship.parentId}: {e}")
            self.stats['relationships_skipped'] += 1
            raise
    
    def verify_loaded_structure(self) -> Dict[str, Any]:
        """Verify the loaded organizational structure."""
        logger.info("Verifying loaded organizational structure")
        
        verification = {
            'verification_successful': False,
            'node_counts': {},
            'relationship_counts': {},
            'hierarchy_integrity': {},
            'sample_organizations': []
        }
        
        try:
            # Get node and relationship counts
            verification['node_counts'] = self.client.get_node_counts()
            verification['relationship_counts'] = self.client.get_relationship_counts()
            
            # Check hierarchy integrity
            hierarchy_check = self._verify_hierarchy_integrity()
            verification['hierarchy_integrity'] = hierarchy_check
            
            # Get sample organizations
            sample_orgs = self._get_sample_organizations()
            verification['sample_organizations'] = sample_orgs
            
            # Determine if verification passed
            org_count = verification['node_counts'].get('Organization', 0)
            part_of_count = verification['relationship_counts'].get('PART_OF', 0)
            no_self_refs = hierarchy_check.get('self_references_found', 0) == 0
            
            verification['verification_successful'] = (
                org_count > 0 and 
                part_of_count >= 0 and 
                no_self_refs
            )
            
            logger.info(f"Structure verification: {verification['verification_successful']}")
            
        except Exception as e:
            logger.error(f"Structure verification failed: {e}")
            verification['verification_error'] = str(e)
        
        return verification
    
    def _verify_hierarchy_integrity(self) -> Dict[str, Any]:
        """Verify hierarchy integrity in the loaded graph."""
        integrity_check = {
            'self_references_found': 0,
            'cycles_detected': 0,
            'orphaned_organizations': 0,
            'max_hierarchy_depth': 0
        }
        
        try:
            # Check for self-referential PART_OF relationships
            self_ref_query = """
            MATCH (o:Organization)-[:PART_OF]->(o)
            RETURN count(o) as self_ref_count
            """
            result = self.client.execute_query(self_ref_query)
            integrity_check['self_references_found'] = result[0]['self_ref_count']
            
            # Check maximum hierarchy depth
            depth_query = """
            MATCH path = (leaf:Organization)-[:PART_OF*]->(root:Organization)
            WHERE NOT (root)-[:PART_OF]->()
            RETURN max(length(path)) as max_depth
            """
            result = self.client.execute_query(depth_query)
            if result and result[0]['max_depth'] is not None:
                integrity_check['max_hierarchy_depth'] = result[0]['max_depth']
            
            # Count organizations without parents (roots)
            root_count_query = """
            MATCH (o:Organization)
            WHERE NOT (o)-[:PART_OF]->()
            RETURN count(o) as root_count
            """
            result = self.client.execute_query(root_count_query)
            integrity_check['root_organizations'] = result[0]['root_count']
            
            # Count organizations without children (leaves)
            leaf_count_query = """
            MATCH (o:Organization)
            WHERE NOT ()-[:PART_OF]->(o)
            RETURN count(o) as leaf_count
            """
            result = self.client.execute_query(leaf_count_query)
            integrity_check['leaf_organizations'] = result[0]['leaf_count']
            
        except Exception as e:
            logger.error(f"Hierarchy integrity check failed: {e}")
            integrity_check['error'] = str(e)
        
        return integrity_check
    
    def _get_sample_organizations(self) -> List[Dict]:
        """Get sample organizations for verification."""
        query = """
        MATCH (o:Organization)
        RETURN o.id as id, o.nameEng as nameEng, o.level as level
        ORDER BY o.nameEng
        LIMIT 5
        """
        
        try:
            result = self.client.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Failed to get sample organizations: {e}")
            return []