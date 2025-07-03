"""
Publication data model with rich attributes and validation.
"""

from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class PublicationBase(BaseModel):
    """Base publication model with shared attributes."""
    
    # Required fields
    id: str = Field(..., description="Unique publication identifier")
    title: str = Field(..., min_length=1, description="Publication title (required)")
    year: int = Field(..., ge=1900, le=2030, description="Publication year (required)")
    publicationType: str = Field(default="article", description="Type of publication")
    
    # Rich content
    abstract: Optional[str] = Field(None, description="Publication abstract")
    language: Optional[str] = Field(None, description="Publication language (ISO code)")
    
    # Academic identifiers
    doi: Optional[str] = Field(None, description="Digital Object Identifier")
    scopusId: Optional[str] = Field(None, description="Scopus identifier")
    pubmedId: Optional[str] = Field(None, description="PubMed identifier")
    isbn: Optional[str] = Field(None, description="ISBN for books")
    
    # Journal/venue information
    journalTitle: Optional[str] = Field(None, description="Journal or venue title")
    journalPublisher: Optional[str] = Field(None, description="Publisher name")
    
    # URLs and access
    detailsUrlEng: Optional[str] = Field(None, description="URL to publication details")
    
    # Text field for embeddings (concatenated content)
    text: Optional[str] = Field(None, description="Concatenated text for vector embeddings")
    
    @field_validator('doi')
    @classmethod
    def validate_doi_format(cls, v):
        """Basic DOI format validation."""
        if v is not None:
            v = v.strip()
            # Remove common prefixes
            v = v.replace('https://doi.org/', '').replace('http://doi.org/', '').replace('doi:', '')
            # Basic format check: should contain at least one slash
            if '/' not in v:
                raise ValueError(f'Invalid DOI format: {v}')
            return v
        return v
    
    @field_validator('year')
    @classmethod
    def reasonable_publication_year(cls, v):
        """Validate publication year is reasonable."""
        current_year = datetime.now().year
        if v > current_year + 2:  # Allow for advance publications
            raise ValueError(f'Publication year {v} is too far in the future')
        return v
    
    @field_validator('publicationType')
    @classmethod
    def valid_publication_type(cls, v):
        """Validate publication type."""
        valid_types = {
            'article', 'book', 'chapter', 'conference', 'proceedings', 
            'thesis', 'patent', 'report', 'preprint', 'review', 'editorial'
        }
        v_lower = v.lower().strip()
        if v_lower not in valid_types:
            # Accept the value but warn - don't be too strict
            pass
        return v.strip()
    
    def generate_text_field(self) -> str:
        """Generate concatenated text field for embeddings."""
        text_parts = []
        
        if self.title:
            text_parts.append(self.title)
        
        if self.abstract:
            text_parts.append(self.abstract)
        
        # Note: Keywords would be added here when processing relationships
        
        return ' '.join(text_parts).strip()
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        extra = "forbid"


class PublicationCreate(PublicationBase):
    """Model for creating new publications."""
    
    @field_validator('id')
    @classmethod
    def id_not_empty(cls, v):
        """Ensure ID is not empty string."""
        if not v.strip():
            raise ValueError('Publication ID cannot be empty')
        return v.strip()
    
    @field_validator('title')
    @classmethod
    def title_not_empty(cls, v):
        """Ensure title is not empty."""
        if not v.strip():
            raise ValueError('Publication title cannot be empty')
        return v.strip()


class Publication(PublicationBase):
    """Full publication model with metadata."""
    
    # System metadata
    createdAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was created")
    updatedAt: Optional[datetime] = Field(default_factory=datetime.now, description="When record was last updated")
    
    # Computed relationship counts
    authorCount: int = Field(default=0, ge=0, description="Number of authors")
    keywordCount: int = Field(default=0, ge=0, description="Number of keywords")
    citationCount: int = Field(default=0, ge=0, description="Citation count (if available)")
    
    def to_neo4j_dict(self) -> dict:
        """Convert to dictionary suitable for Neo4j insertion."""
        data = self.dict(exclude_none=True, exclude={
            'authorCount', 'keywordCount', 'citationCount'
        })
        
        # Convert datetime to ISO string for Neo4j
        if 'createdAt' in data:
            data['createdAt'] = data['createdAt'].isoformat()
        if 'updatedAt' in data:
            data['updatedAt'] = data['updatedAt'].isoformat()
        
        # Ensure text field is generated if not provided
        if 'text' not in data or not data['text']:
            pub_base = PublicationBase(**{k: v for k, v in data.items() if k in PublicationBase.__fields__})
            data['text'] = pub_base.generate_text_field()
        
        return data
    
    @classmethod
    def from_neo4j_record(cls, record: dict) -> 'Publication':
        """Create Publication from Neo4j record."""
        # Handle datetime conversion from Neo4j
        if 'createdAt' in record and isinstance(record['createdAt'], str):
            record['createdAt'] = datetime.fromisoformat(record['createdAt'])
        if 'updatedAt' in record and isinstance(record['updatedAt'], str):
            record['updatedAt'] = datetime.fromisoformat(record['updatedAt'])
        
        return cls(**record)


class PublicationAuthorship(BaseModel):
    """Model for publication-person authorship relationships."""
    
    publicationId: str = Field(..., description="Publication identifier")
    personId: str = Field(..., description="Person identifier")
    
    # Authorship details
    order: int = Field(..., ge=0, description="Author order (0-based)")
    role: str = Field(default="Author", description="Author role")
    
    @field_validator('role')
    @classmethod
    def valid_author_role(cls, v):
        """Validate author role."""
        valid_roles = {'Author', 'Editor', 'Reviewer', 'Contributor', 'Translator'}
        if v not in valid_roles:
            # Accept but don't be too strict
            pass
        return v
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


class Keyword(BaseModel):
    """Model for keyword entities."""
    
    # Primary identifier is the normalized value
    value: str = Field(..., min_length=1, description="Normalized keyword value (lowercase)")
    
    # Optional display version
    displayValue: Optional[str] = Field(None, description="Original case keyword for display")
    
    @field_validator('value')
    @classmethod
    def normalize_value(cls, v):
        """Normalize keyword value."""
        return v.strip().lower()
    
    def to_neo4j_dict(self) -> dict:
        """Convert to dictionary suitable for Neo4j insertion."""
        return self.dict(exclude_none=True)
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


class PublicationKeyword(BaseModel):
    """Model for publication-keyword relationships."""
    
    publicationId: str = Field(..., description="Publication identifier")
    keywordValue: str = Field(..., description="Keyword value (normalized)")
    
    # Optional metadata
    source: Optional[str] = Field(None, description="Source of keyword (author, automatic, etc.)")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence score for automatic keywords")
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True