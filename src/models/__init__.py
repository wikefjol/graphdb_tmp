"""
Data models with rich attributes and validation for knowledge graph entities.
"""

from .organization import Organization, OrganizationCreate
from .person import Person, PersonCreate  
from .publication import Publication, PublicationCreate

__all__ = [
    'Organization', 'OrganizationCreate',
    'Person', 'PersonCreate', 
    'Publication', 'PublicationCreate'
]