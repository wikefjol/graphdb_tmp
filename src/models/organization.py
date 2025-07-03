"""
Organization data model with rich attributes and validation.
"""

from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class OrganizationBase(BaseModel):
    """Base organization model with shared attributes."""
    
    # Required fields
    id: str = Field(..., description="Unique organization identifier")
    nameEng: str = Field(..., min_length=1, description="English name (required)")
    
    # Optional rich attributes
    nameSwe: Optional[str] = Field(None, description="Swedish name")
    displayNameEng: Optional[str] = Field(None, description="Display name in English")
    displayNameSwe: Optional[str] = Field(None, description="Display name in Swedish")
    displayPathEng: Optional[str] = Field(None, description="Full organizational path in English")
    displayPathSwe: Optional[str] = Field(None, description="Full organizational path in Swedish")
    
    # Organizational hierarchy
    level: Optional[str] = Field(None, description="Organizational level: university, department, sub_department, unit, etc.")
    organizationType: str = Field(default="academic", description="Type of organization")
    
    # Geographic information
    city: Optional[str] = Field(None, description="City location")
    country: Optional[str] = Field(None, description="Country location")
    geoLat: Optional[float] = Field(None, ge=-90, le=90, description="Latitude coordinate")
    geoLong: Optional[float] = Field(None, ge=-180, le=180, description="Longitude coordinate")
    
    # Temporal information
    startYear: Optional[int] = Field(None, ge=1000, le=3000, description="Year organization was established")
    endYear: Optional[int] = Field(None, ge=1000, le=3000, description="Year organization was dissolved (if applicable)")
    
    @field_validator('endYear')
    @classmethod
    def end_year_after_start_year(cls, v, info):
        """Validate that end year is after start year."""
        if v is not None and info.data and 'startYear' in info.data and info.data['startYear'] is not None:
            if v <= info.data['startYear']:
                raise ValueError('End year must be after start year')
        return v
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        extra = "forbid"  # Prevent extra fields


class OrganizationCreate(OrganizationBase):
    """Model for creating new organizations."""
    
    # Additional validation for creation
    @field_validator('id')
    @classmethod
    def id_not_empty(cls, v):
        """Ensure ID is not empty string."""
        if not v.strip():
            raise ValueError('Organization ID cannot be empty')
        return v.strip()
    
    @field_validator('nameEng')
    @classmethod
    def name_not_empty(cls, v):
        """Ensure English name is not empty."""
        if not v.strip():
            raise ValueError('Organization nameEng cannot be empty')
        return v.strip()


class Organization(OrganizationBase):
    """Full organization model with metadata."""
    
    # Computed fields (set by system)
    createdAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was created")
    updatedAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was last updated")
    
    # Relationship tracking (computed)
    hasChildren: bool = Field(default=False, description="Whether this organization has child organizations")
    hasParent: bool = Field(default=False, description="Whether this organization has a parent organization")
    childCount: int = Field(default=0, ge=0, description="Number of child organizations")
    
    def to_neo4j_dict(self) -> dict:
        """Convert to dictionary suitable for Neo4j insertion."""
        data = self.dict(exclude_none=True, exclude={'hasChildren', 'hasParent', 'childCount'})
        
        # Convert datetime to ISO string for Neo4j
        if 'createdAt' in data:
            data['createdAt'] = data['createdAt'].isoformat()
        if 'updatedAt' in data:
            data['updatedAt'] = data['updatedAt'].isoformat()
        
        return data
    
    @classmethod
    def from_neo4j_record(cls, record: dict) -> 'Organization':
        """Create Organization from Neo4j record."""
        # Handle datetime conversion from Neo4j
        if 'createdAt' in record and isinstance(record['createdAt'], str):
            record['createdAt'] = datetime.fromisoformat(record['createdAt'])
        if 'updatedAt' in record and isinstance(record['updatedAt'], str):
            record['updatedAt'] = datetime.fromisoformat(record['updatedAt'])
        
        return cls(**record)


class OrganizationHierarchy(BaseModel):
    """Model for organizational hierarchy relationships."""
    
    childId: str = Field(..., description="Child organization ID")
    parentId: str = Field(..., description="Parent organization ID")
    
    # Relationship metadata
    relationshipType: str = Field(default="PART_OF", description="Type of hierarchical relationship")
    level: Optional[int] = Field(None, ge=0, description="Hierarchy level (0=root)")
    
    @field_validator('parentId')
    @classmethod
    def no_self_reference(cls, v, info):
        """Prevent self-referential relationships."""
        if info.data and 'childId' in info.data and v == info.data['childId']:
            raise ValueError('Organization cannot be parent of itself (prevents self-referential PART_OF relationships)')
        return v
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


# Utility functions for common operations
def validate_organization_hierarchy(relationships: List[OrganizationHierarchy]) -> List[str]:
    """Validate organizational hierarchy for cycles and other issues."""
    errors = []
    
    # Check for self-references (should be caught by model validation, but double-check)
    for rel in relationships:
        if rel.childId == rel.parentId:
            errors.append(f"Self-reference detected: {rel.childId} -> {rel.parentId}")
    
    # Check for potential cycles (simplified check)
    parent_map = {rel.childId: rel.parentId for rel in relationships}
    
    for child_id in parent_map:
        visited = set()
        current = child_id
        
        while current in parent_map and current not in visited:
            visited.add(current)
            current = parent_map[current]
            
            if current == child_id:
                errors.append(f"Cycle detected involving organization: {child_id}")
                break
    
    return errors