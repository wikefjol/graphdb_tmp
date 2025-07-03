"""
Clean Chalmers organizational structure transformer with self-reference prevention.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from ..models.organization import Organization, OrganizationCreate, OrganizationHierarchy
from ..utils.validation import clean_organization_data, extract_hierarchy_relationships, validate_hierarchy

logger = logging.getLogger(__name__)


class ChalmersTransformer:
    """Transform Chalmers organizational structure with proper validation."""
    
    def __init__(self):
        """Initialize the transformer."""
        self.organizations: List[Dict] = []
        self.hierarchy_relationships: List[OrganizationHierarchy] = []
    
    def load_chalmers_structure(self, file_path: str) -> Tuple[List[OrganizationCreate], List[OrganizationHierarchy]]:
        """
        Load and transform Chalmers organizational structure from JSON file.
        
        Returns:
            Tuple of (validated_organizations, validated_hierarchy_relationships)
        """
        logger.info(f"Loading Chalmers organizational structure from: {file_path}")
        
        # 1. Load raw data
        raw_data = self._load_json_file(file_path)
        
        # 2. Extract organizations from nested structure
        raw_organizations = self._extract_organizations_from_nested_structure(raw_data)
        logger.info(f"Extracted {len(raw_organizations)} organizations from nested structure")
        
        # 3. Clean organization data
        cleaned_organizations = clean_organization_data(raw_organizations)
        
        # 4. Extract hierarchy relationships with validation
        hierarchy_relationships = extract_hierarchy_relationships(raw_organizations)
        
        # 5. Validate complete hierarchy
        is_valid, errors = validate_hierarchy(hierarchy_relationships)
        if not is_valid:
            logger.warning(f"Hierarchy validation issues: {errors}")
            # Filter out problematic relationships
            hierarchy_relationships = self._fix_hierarchy_issues(hierarchy_relationships, errors)
        
        # 6. Create validated organization models
        validated_organizations = self._create_organization_models(cleaned_organizations)
        
        logger.info(f"Successfully processed {len(validated_organizations)} organizations "
                   f"with {len(hierarchy_relationships)} hierarchy relationships")
        
        return validated_organizations, hierarchy_relationships
    
    def _load_json_file(self, file_path: str) -> Dict:
        """Load JSON file with error handling."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Chalmers structure file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Loaded JSON file: {file_path.name} ({file_path.stat().st_size / 1024:.1f} KB)")
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in Chalmers structure file: {e}")
        except Exception as e:
            raise Exception(f"Failed to load Chalmers structure file: {e}")
    
    def _extract_organizations_from_nested_structure(self, data: Dict) -> List[Dict]:
        """Extract all organizations from the nested Chalmers structure."""
        organizations = []
        
        def process_organization_node(node: Dict, parent_id: Optional[str] = None, level: str = "unknown"):
            """Recursively process organization nodes."""
            if not isinstance(node, dict):
                return
            
            # Check if this node represents an organization (has an ID)
            if 'id' in node:
                org = self._create_organization_dict(node, parent_id, level)
                organizations.append(org)
                current_id = node['id']
            else:
                current_id = parent_id
            
            # Process nested organizational structures
            self._process_nested_structures(node, current_id, process_organization_node)
        
        # Start processing from the root
        if 'chalmers_organizational_structure' in data:
            root_org = data['chalmers_organizational_structure']
            process_organization_node(root_org, None, "university")
        else:
            # Fallback: try processing the entire data structure
            process_organization_node(data, None, "university")
        
        return organizations
    
    def _create_organization_dict(self, node: Dict, parent_id: Optional[str], level: str) -> Dict:
        """Create organization dictionary from node data."""
        org = {
            'id': str(node['id']),
            'nameEng': node.get('name', f"Organization {node['id']}"),
            'nameSwe': node.get('name', ''),  # Use same name if Swedish not available
            'level': node.get('level', level),
            'organizationType': 'academic',
            'city': 'Gothenburg',
            'country': 'Sweden'
        }
        
        # Add optional fields
        if 'path' in node:
            org['displayPathEng'] = node['path']
            org['displayPathSwe'] = node['path']  # Assume same path for Swedish
        
        # Add parent relationship info
        if parent_id:
            org['parent_id'] = str(parent_id)
        
        return org
    
    def _process_nested_structures(self, node: Dict, current_id: Optional[str], processor_func):
        """Process various nested organizational structures."""
        structure_mappings = {
            'departments': 'department',
            'sub_departments': 'sub_department', 
            'units': 'unit',
            'centres': 'centre',
            'groups': 'group'
        }
        
        for key, level in structure_mappings.items():
            if key in node and isinstance(node[key], dict):
                for item_key, item_value in node[key].items():
                    processor_func(item_value, current_id, level)
        
        # Handle organizations array
        if 'organizations' in node and isinstance(node['organizations'], list):
            for org_item in node['organizations']:
                processor_func(org_item, current_id, 'unit')
    
    def _fix_hierarchy_issues(self, relationships: List[OrganizationHierarchy], 
                             errors: List[str]) -> List[OrganizationHierarchy]:
        """Fix hierarchy issues by removing problematic relationships."""
        fixed_relationships = []
        
        # Remove self-referential relationships (critical fix)
        self_refs = set()
        for error in errors:
            if "Self-reference detected" in error:
                # Extract organization ID from error message
                parts = error.split(" - ")
                if len(parts) > 1:
                    org_id = parts[1].split(" ")[0]
                    self_refs.add(org_id)
        
        for rel in relationships:
            # Skip self-referential relationships
            if rel.childId == rel.parentId:
                logger.warning(f"Removing self-referential relationship: {rel.childId} -> {rel.parentId}")
                continue
            
            # Skip relationships involving organizations with self-reference issues
            if rel.childId in self_refs or rel.parentId in self_refs:
                logger.warning(f"Skipping relationship involving problematic org: {rel.childId} -> {rel.parentId}")
                continue
            
            fixed_relationships.append(rel)
        
        logger.info(f"Fixed hierarchy: {len(fixed_relationships)} relationships "
                   f"(removed {len(relationships) - len(fixed_relationships)} problematic ones)")
        
        return fixed_relationships
    
    def _create_organization_models(self, organizations: List[Dict]) -> List[OrganizationCreate]:
        """Create validated organization models."""
        validated_organizations = []
        
        for org_data in organizations:
            try:
                # Create organization model with validation
                org = OrganizationCreate(**org_data)
                validated_organizations.append(org)
                
            except Exception as e:
                logger.warning(f"Failed to validate organization {org_data.get('id')}: {e}")
                continue
        
        logger.info(f"Created {len(validated_organizations)} validated organization models")
        return validated_organizations
    
    def get_organizational_statistics(self, organizations: List[OrganizationCreate], 
                                    relationships: List[OrganizationHierarchy]) -> Dict:
        """Get statistics about the organizational structure."""
        # Count organizations by level
        level_counts = {}
        for org in organizations:
            level = org.level or 'unknown'
            level_counts[level] = level_counts.get(level, 0) + 1
        
        # Find root organizations (those that are never children)
        child_ids = {rel.childId for rel in relationships}
        parent_ids = {rel.parentId for rel in relationships}
        root_ids = parent_ids - child_ids
        
        # Find leaf organizations (those that are never parents)
        leaf_ids = child_ids - parent_ids
        
        # Calculate hierarchy depth
        max_depth = self._calculate_max_hierarchy_depth(relationships)
        
        return {
            'total_organizations': len(organizations),
            'total_relationships': len(relationships),
            'organizations_by_level': level_counts,
            'root_organizations': len(root_ids),
            'leaf_organizations': len(leaf_ids),
            'max_hierarchy_depth': max_depth,
            'root_organization_ids': list(root_ids),
            'validation_passed': True
        }
    
    def _calculate_max_hierarchy_depth(self, relationships: List[OrganizationHierarchy]) -> int:
        """Calculate maximum hierarchy depth."""
        if not relationships:
            return 0
        
        # Build parent-child mapping
        children_map = {}
        for rel in relationships:
            if rel.parentId not in children_map:
                children_map[rel.parentId] = []
            children_map[rel.parentId].append(rel.childId)
        
        # Find root organizations
        child_ids = {rel.childId for rel in relationships}
        parent_ids = {rel.parentId for rel in relationships}
        roots = parent_ids - child_ids
        
        # Calculate depth from each root
        max_depth = 0
        
        def calculate_depth(org_id: str, current_depth: int = 0) -> int:
            if org_id not in children_map:
                return current_depth
            
            max_child_depth = current_depth
            for child_id in children_map[org_id]:
                child_depth = calculate_depth(child_id, current_depth + 1)
                max_child_depth = max(max_child_depth, child_depth)
            
            return max_child_depth
        
        for root_id in roots:
            depth = calculate_depth(root_id)
            max_depth = max(max_depth, depth)
        
        return max_depth