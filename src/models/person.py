"""
Person data model with rich attributes and validation.
"""

from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date


class PersonBase(BaseModel):
    """Base person model with shared attributes."""
    
    # Required fields
    id: str = Field(..., description="Unique person identifier")
    displayName: str = Field(..., min_length=1, description="Display name (required)")
    
    # Name components
    firstName: Optional[str] = Field(None, description="Given name")
    lastName: Optional[str] = Field(None, description="Family name")
    
    # Personal information
    birthYear: Optional[int] = Field(None, ge=1900, le=2020, description="Birth year")
    email: Optional[str] = Field(None, description="Contact email address")
    
    # Academic identifiers (rich identifier support)
    orcid: Optional[str] = Field(None, description="ORCID identifier")
    scopusAuthorId: Optional[str] = Field(None, description="Scopus author identifier") 
    cid: Optional[str] = Field(None, description="Institutional CID identifier")
    
    @field_validator('orcid')
    @classmethod
    def validate_orcid_format(cls, v):
        """Validate ORCID format (simplified)."""
        if v is not None:
            # Remove common prefixes and clean up
            v = v.replace('https://orcid.org/', '').replace('http://orcid.org/', '').strip()
            # Basic format check: XXXX-XXXX-XXXX-XXXX
            if len(v) == 19 and v.count('-') == 3:
                return v
            elif len(v) == 16 and '-' not in v:
                # Format without dashes - add them
                return f"{v[:4]}-{v[4:8]}-{v[8:12]}-{v[12:16]}"
            else:
                raise ValueError(f'Invalid ORCID format: {v}')
        return v
    
    @field_validator('birthYear')
    @classmethod
    def reasonable_birth_year(cls, v):
        """Validate birth year is reasonable."""
        if v is not None:
            current_year = datetime.now().year
            if v > current_year - 15:  # Minimum reasonable age for publications
                raise ValueError(f'Birth year {v} seems too recent')
        return v
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        extra = "forbid"


class PersonCreate(PersonBase):
    """Model for creating new persons."""
    
    @field_validator('id')
    @classmethod
    def id_not_empty(cls, v):
        """Ensure ID is not empty string."""
        if not v.strip():
            raise ValueError('Person ID cannot be empty')
        return v.strip()
    
    @field_validator('displayName')
    @classmethod
    def display_name_not_empty(cls, v):
        """Ensure display name is not empty."""
        if not v.strip():
            raise ValueError('Person displayName cannot be empty')
        return v.strip()


class Person(PersonBase):
    """Full person model with metadata."""
    
    # System metadata
    createdAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was created")
    updatedAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was last updated")
    
    # Computed relationship counts
    publicationCount: int = Field(default=0, ge=0, description="Number of authored publications")
    collaboratorCount: int = Field(default=0, ge=0, description="Number of unique collaborators")
    affiliationCount: int = Field(default=0, ge=0, description="Number of organizational affiliations")
    
    def to_neo4j_dict(self) -> dict:
        """Convert to dictionary suitable for Neo4j insertion."""
        data = self.dict(exclude_none=True, exclude={
            'publicationCount', 'collaboratorCount', 'affiliationCount'
        })
        
        # Convert datetime to ISO string for Neo4j
        if 'createdAt' in data:
            data['createdAt'] = data['createdAt'].isoformat()
        if 'updatedAt' in data:
            data['updatedAt'] = data['updatedAt'].isoformat()
        
        return data
    
    @classmethod
    def from_neo4j_record(cls, record: dict) -> 'Person':
        """Create Person from Neo4j record."""
        # Handle datetime conversion from Neo4j
        if 'createdAt' in record and isinstance(record['createdAt'], str):
            record['createdAt'] = datetime.fromisoformat(record['createdAt'])
        if 'updatedAt' in record and isinstance(record['updatedAt'], str):
            record['updatedAt'] = datetime.fromisoformat(record['updatedAt'])
        
        return cls(**record)


class PersonAffiliation(BaseModel):
    """Model for person-organization affiliation relationships."""
    
    personId: str = Field(..., description="Person identifier")
    organizationId: str = Field(..., description="Organization identifier")
    
    # Affiliation details
    title: Optional[str] = Field(None, description="Position title")
    priority: Optional[str] = Field(None, description="Affiliation priority (Primary, Secondary, etc.)")
    
    # Temporal information - KEEP ALL HISTORICAL AFFILIATIONS
    startDate: Optional[date] = Field(None, description="Affiliation start date")
    endDate: Optional[date] = Field(None, description="Affiliation end date (null for ongoing)")
    
    @field_validator('endDate')
    @classmethod
    def end_date_after_start_date(cls, v, info):
        """Validate that end date is after start date."""
        if v is not None and info.data and 'startDate' in info.data and info.data['startDate'] is not None:
            if v <= info.data['startDate']:
                raise ValueError('End date must be after start date')
        return v
    
    def is_current_affiliation(self) -> bool:
        """Check if this is a current (ongoing) affiliation."""
        return self.endDate is None or self.endDate >= date.today()
    
    def is_primary_affiliation(self) -> bool:
        """Check if this is a primary affiliation."""
        return self.priority is not None and self.priority.lower() == 'primary'
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


class PersonCollaboration(BaseModel):
    """Model for person-person collaboration relationships."""
    
    person1Id: str = Field(..., description="First person identifier")
    person2Id: str = Field(..., description="Second person identifier")
    
    # Collaboration metrics
    publicationCount: int = Field(default=0, ge=0, description="Number of joint publications")
    firstCollaboration: Optional[int] = Field(None, description="Year of first collaboration")
    lastCollaboration: Optional[int] = Field(None, description="Year of most recent collaboration")
    
    @field_validator('person2Id')
    @classmethod
    def no_self_collaboration(cls, v, info):
        """Prevent self-collaboration relationships."""
        if info.data and 'person1Id' in info.data and v == info.data['person1Id']:
            raise ValueError('Person cannot collaborate with themselves')
        return v
    
    @field_validator('lastCollaboration')
    @classmethod
    def last_after_first(cls, v, info):
        """Validate collaboration year order."""
        if v is not None and info.data and 'firstCollaboration' in info.data and info.data['firstCollaboration'] is not None:
            if v < info.data['firstCollaboration']:
                raise ValueError('Last collaboration year must be >= first collaboration year')
        return v
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True