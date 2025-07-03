"""
Validation utilities for data integrity and relationship validation.
"""

from typing import List, Dict, Set, Tuple, Optional
import logging

from ..models.organization import OrganizationHierarchy

logger = logging.getLogger(__name__)


def validate_hierarchy(relationships: List[OrganizationHierarchy]) -> Tuple[bool, List[str]]:
    """
    Validate organizational hierarchy for cycles and self-references.
    
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # 1. Check for self-references (critical issue to fix)
    self_refs = []
    for rel in relationships:
        if rel.childId == rel.parentId:
            self_refs.append(rel.childId)
            errors.append(f"CRITICAL: Self-reference detected - {rel.childId} is parent of itself")
    
    # 2. Build parent-child mapping for cycle detection
    parent_map: Dict[str, str] = {}
    child_counts: Dict[str, int] = {}
    
    for rel in relationships:
        # Skip self-references for cycle detection
        if rel.childId != rel.parentId:
            parent_map[rel.childId] = rel.parentId
            child_counts[rel.parentId] = child_counts.get(rel.parentId, 0) + 1
    
    # 3. Detect cycles using path traversal
    cycles_detected = set()
    
    for child_id in parent_map:
        if child_id in cycles_detected:
            continue
            
        visited = set()
        path = []
        current = child_id
        
        # Traverse up the hierarchy
        while current in parent_map:
            if current in visited:
                # Cycle detected
                cycle_start = path.index(current)
                cycle_path = path[cycle_start:] + [current]
                cycle_str = " -> ".join(cycle_path)
                errors.append(f"Cycle detected: {cycle_str}")
                cycles_detected.update(cycle_path)
                break
            
            visited.add(current)
            path.append(current)
            current = parent_map[current]
    
    # 4. Check for multiple parents (not necessarily an error, but worth noting)
    children_with_multiple_parents = []
    child_parent_count = {}
    
    for rel in relationships:
        if rel.childId != rel.parentId:  # Skip self-references
            child_parent_count[rel.childId] = child_parent_count.get(rel.childId, 0) + 1
    
    for child_id, parent_count in child_parent_count.items():
        if parent_count > 1:
            children_with_multiple_parents.append(f"{child_id} has {parent_count} parents")
    
    if children_with_multiple_parents:
        errors.append(f"WARNING: Organizations with multiple parents: {'; '.join(children_with_multiple_parents)}")
    
    # 5. Check for orphaned organizations (those that are parents but have no parents themselves)
    all_child_ids = {rel.childId for rel in relationships if rel.childId != rel.parentId}
    all_parent_ids = {rel.parentId for rel in relationships if rel.childId != rel.parentId}
    root_organizations = all_parent_ids - all_child_ids
    
    logger.info(f"Hierarchy validation: {len(root_organizations)} root organizations found")
    
    is_valid = len(errors) == 0 or all("WARNING:" in error for error in errors)
    
    return is_valid, errors


def prevent_cycles(existing_relationships: List[OrganizationHierarchy], 
                  new_relationship: OrganizationHierarchy) -> Tuple[bool, Optional[str]]:
    """
    Check if adding a new relationship would create a cycle.
    
    Returns:
        Tuple of (is_safe_to_add, error_message)
    """
    # 1. Check for self-reference
    if new_relationship.childId == new_relationship.parentId:
        return False, f"Cannot add self-reference: {new_relationship.childId} -> {new_relationship.parentId}"
    
    # 2. Build existing parent-child mapping
    parent_map = {}
    for rel in existing_relationships:
        if rel.childId != rel.parentId:  # Skip any existing self-references
            parent_map[rel.childId] = rel.parentId
    
    # 3. Check if new relationship would create cycle
    # Starting from the new parent, traverse up to see if we reach the new child
    current = new_relationship.parentId
    visited = set()
    path = [new_relationship.childId, new_relationship.parentId]
    
    while current in parent_map and current not in visited:
        visited.add(current)
        current = parent_map[current]
        path.append(current)
        
        if current == new_relationship.childId:
            cycle_path = " -> ".join(path)
            return False, f"Adding relationship would create cycle: {cycle_path}"
    
    return True, None


def validate_relationship(relationship_type: str, source_id: str, target_id: str, 
                         metadata: Optional[Dict] = None) -> Tuple[bool, Optional[str]]:
    """
    Validate a general relationship for common issues.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # 1. Check for empty IDs
    if not source_id or not source_id.strip():
        return False, "Source ID cannot be empty"
    
    if not target_id or not target_id.strip():
        return False, "Target ID cannot be empty"
    
    # 2. Check for self-relationships in types where it doesn't make sense
    problematic_self_relationships = {
        'PART_OF', 'COLLABORATES_WITH', 'AFFILIATED_WITH'
    }
    
    if relationship_type in problematic_self_relationships and source_id == target_id:
        return False, f"Self-referential {relationship_type} relationship not allowed: {source_id}"
    
    # 3. Relationship-specific validation
    if relationship_type == "PART_OF":
        # Additional PART_OF specific checks
        if source_id == target_id:
            return False, f"Organization cannot be part of itself: {source_id}"
    
    elif relationship_type == "AUTHORED":
        # Check authorship metadata
        if metadata and 'order' in metadata:
            if not isinstance(metadata['order'], int) or metadata['order'] < 0:
                return False, "Author order must be non-negative integer"
    
    elif relationship_type == "AFFILIATED_WITH":
        # Check affiliation metadata
        if metadata and 'startDate' in metadata and 'endDate' in metadata:
            if metadata['startDate'] and metadata['endDate']:
                if metadata['endDate'] <= metadata['startDate']:
                    return False, "Affiliation end date must be after start date"
    
    return True, None


def clean_organization_data(organizations: List[Dict]) -> List[Dict]:
    """
    Clean organization data to prevent common issues.
    
    Returns:
        List of cleaned organization dictionaries
    """
    cleaned = []
    seen_ids = set()
    
    for org in organizations:
        # Skip organizations without valid IDs
        if not org.get('id') or not str(org['id']).strip():
            logger.warning(f"Skipping organization without valid ID: {org}")
            continue
        
        org_id = str(org['id']).strip()
        
        # Skip duplicates
        if org_id in seen_ids:
            logger.warning(f"Skipping duplicate organization ID: {org_id}")
            continue
        
        seen_ids.add(org_id)
        
        # Clean the organization data
        clean_org = {
            'id': org_id,
            'nameEng': str(org.get('nameEng', '')).strip() or f"Organization {org_id}",
        }
        
        # Add optional fields if present and non-empty
        optional_fields = [
            'nameSwe', 'displayNameEng', 'displayNameSwe', 
            'displayPathEng', 'displayPathSwe', 'level', 'organizationType',
            'city', 'country', 'geoLat', 'geoLong', 'startYear', 'endYear'
        ]
        
        for field in optional_fields:
            if field in org and org[field] is not None:
                value = org[field]
                
                # Handle numeric fields
                if field in ['geoLat', 'geoLong'] and value != '':
                    try:
                        clean_org[field] = float(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {field} for {org_id}: {value}")
                        continue
                
                elif field in ['startYear', 'endYear'] and value != '':
                    try:
                        clean_org[field] = int(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {field} for {org_id}: {value}")
                        continue
                
                # Handle string fields
                elif isinstance(value, str) and value.strip():
                    clean_org[field] = value.strip()
                elif value and not isinstance(value, str):
                    clean_org[field] = value
        
        cleaned.append(clean_org)
    
    logger.info(f"Cleaned {len(cleaned)} organizations from {len(organizations)} input organizations")
    return cleaned


def extract_hierarchy_relationships(organizations: List[Dict]) -> List[OrganizationHierarchy]:
    """
    Extract PART_OF relationships from organization data with validation.
    
    Returns:
        List of validated OrganizationHierarchy objects
    """
    relationships = []
    
    for org in organizations:
        org_id = org.get('id')
        parent_id = org.get('parent_id')
        
        if org_id and parent_id and org_id != parent_id:
            try:
                rel = OrganizationHierarchy(
                    childId=str(org_id),
                    parentId=str(parent_id)
                )
                relationships.append(rel)
            except Exception as e:
                logger.warning(f"Invalid hierarchy relationship {org_id} -> {parent_id}: {e}")
    
    # Validate the entire hierarchy
    is_valid, errors = validate_hierarchy(relationships)
    
    if not is_valid:
        critical_errors = [e for e in errors if "CRITICAL:" in e]
        if critical_errors:
            logger.error(f"Critical hierarchy errors detected: {critical_errors}")
            # Filter out self-references
            relationships = [r for r in relationships if r.childId != r.parentId]
    
    logger.info(f"Extracted {len(relationships)} valid hierarchy relationships")
    return relationships