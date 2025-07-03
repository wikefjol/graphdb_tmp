#!/usr/bin/env python3
"""
Updated Data Transformers for Elasticsearch Integration
Maps ES _id to es_id while preserving display names for visualization
"""

import os
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from neo4j import GraphDatabase
from dotenv import load_dotenv

class UpdatedDataTransformer:
    """Transform Elasticsearch documents to Neo4j format with proper ID mapping"""
    
    @staticmethod
    def transform_person(es_doc: Dict, es_id: str = None) -> Dict:
        """Transform person document from ES to Neo4j format
        
        Args:
            es_doc: The Elasticsearch document
            es_id: The Elasticsearch _id (if not in document)
        """
        # Use provided es_id or extract from document
        doc_id = es_id or es_doc.get('_id') or es_doc.get('Id')
        
        return {
            # Primary key: ES _id
            'es_id': doc_id,
            
            # Display properties for visualization
            'display_name': es_doc.get('DisplayName'),
            'first_name': es_doc.get('FirstName'),
            'last_name': es_doc.get('LastName'),
            
            # Technical properties
            'birth_year': es_doc.get('BirthYear', 0) if es_doc.get('BirthYear') else None,
            'is_active': es_doc.get('IsActive', False),
            'is_deleted': es_doc.get('IsDeleted', False),
            'has_publications': es_doc.get('HasPublications', False),
            'has_projects': es_doc.get('HasProjects', False),
            'has_organization_home': es_doc.get('HasOrganizationHome', False),
            
            # Identifiers
            'orcid': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierOrcid', 'ORCID'),
            'scopus_author_id': UpdatedDataTransformer._extract_identifier(es_doc, 'SCOPUS_AUTHID'),
            'cid': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierCid', 'CID'),
            'cpl_person_id': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierCplPersonId', 'CPL_PERSONID'),
            
            # Metadata
            'created_at': es_doc.get('CreatedAt'),
            'updated_at': es_doc.get('UpdatedAt'),
            'created_by': es_doc.get('CreatedBy'),
            'updated_by': es_doc.get('UpdatedBy'),
            'needs_attention': es_doc.get('NeedsAttention', False),
            
            # Count fields
            'organization_home_count': es_doc.get('OrganizationHomeCount', 0),
            'identifiers_count': es_doc.get('IdentifiersCount', 0),
        }
    
    @staticmethod
    def transform_publication(es_doc: Dict, es_id: str = None) -> Dict:
        """Transform publication document from ES to Neo4j format"""
        doc_id = es_id or es_doc.get('_id') or es_doc.get('Id')
        
        return {
            # Primary key: ES _id
            'es_id': doc_id,
            
            # Display properties for visualization
            'title': es_doc.get('Title'),
            'abstract': es_doc.get('Abstract'),
            
            # Publication details
            'year': es_doc.get('Year'),
            'publication_type': UpdatedDataTransformer._extract_nested_field(es_doc, 'PublicationType', 'NameEng'),
            'publication_type_id': UpdatedDataTransformer._extract_nested_field(es_doc, 'PublicationType', 'Id'),
            'language': UpdatedDataTransformer._extract_nested_field(es_doc, 'Language', 'NameEng'),
            'language_id': UpdatedDataTransformer._extract_nested_field(es_doc, 'Language', 'Id'),
            
            # Identifiers
            'doi': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierDoi', 'DOI'),
            'scopus_id': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierScopusId', 'SCOPUS_ID'),
            'pubmed_id': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierPubmedId', 'PUBMED_ID'),
            'isbn': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierIsbn', 'ISBN'),
            'cpl_pubid': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierCplPubid', 'CPL_PUBID'),
            
            # Status flags
            'is_draft': es_doc.get('IsDraft', False),
            'is_deleted': es_doc.get('IsDeleted', False),
            'is_imported': es_doc.get('IsImported', False),
            'has_organizations': es_doc.get('HasOrganizations', False),
            'has_persons': es_doc.get('HasPersons', False),
            'has_import_errors': es_doc.get('HasImportErrors', False),
            'needs_attention': es_doc.get('NeedsAttention', False),
            
            # Import matching
            'has_import_match_on_scopus_doi': es_doc.get('HasImportMatchOnScopusDoi', False),
            'has_import_match_on_scopus_id': es_doc.get('HasImportMatchOnScopusId', False),
            
            # URLs
            'details_url_eng': es_doc.get('DetailsUrlEng'),
            'details_url_swe': es_doc.get('DetailsUrlSwe'),
            
            # Metadata
            'created_date': es_doc.get('CreatedDate'),
            'updated_date': es_doc.get('UpdatedDate'),
            'created_by': es_doc.get('CreatedBy'),
            'updated_by': es_doc.get('UpdatedBy'),
            'validated_by': es_doc.get('ValidatedBy'),
            'validated_date': es_doc.get('ValidatedDate'),
            'latest_event_date': es_doc.get('LatestEventDate'),
            
            # Chalmers-specific
            'affiliated_ids_chalmers': es_doc.get('AffiliatedIdsChalmers', []),
        }
    
    @staticmethod
    def transform_organization(es_doc: Dict, es_id: str = None) -> Dict:
        """Transform organization document from ES to Neo4j format"""
        doc_id = es_id or es_doc.get('_id') or es_doc.get('Id')
        
        return {
            # Primary key: ES _id
            'es_id': doc_id,
            
            # Display properties for visualization
            'display_name_eng': es_doc.get('DisplayNameEng'),
            'display_name_swe': es_doc.get('DisplayNameSwe'),
            'name_eng': es_doc.get('NameEng'),
            'name_swe': es_doc.get('NameSwe'),
            
            # Hierarchy and structure
            'level': es_doc.get('Level'),
            'display_path_eng': es_doc.get('DisplayPathEng'),
            'display_path_swe': es_doc.get('DisplayPathSwe'),
            'display_path_short_eng': es_doc.get('DisplayPathShortEng'),
            'display_path_short_swe': es_doc.get('DisplayPathShortSwe'),
            
            # Geographic information
            'city': es_doc.get('City'),
            'country': es_doc.get('Country'),
            'postal_no': es_doc.get('PostalNo'),
            'geo_lat': float(es_doc.get('GeoLat')) if es_doc.get('GeoLat') else None,
            'geo_long': float(es_doc.get('GeoLong')) if es_doc.get('GeoLong') else None,
            
            # Temporal information
            'start_year': es_doc.get('StartYear'),
            'end_year': es_doc.get('EndYear'),
            
            # Organization type
            'organization_type': UpdatedDataTransformer._extract_first_org_type(es_doc),
            'organization_type_id': UpdatedDataTransformer._extract_first_org_type_id(es_doc),
            
            # Identifiers
            'ldap_code': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierLdapCode', 'LDAP_CODE'),
            'cpl_department_id': UpdatedDataTransformer._extract_first_or_identifier(es_doc, 'IdentifierCplDepartmentId', 'CPL_DEPARTMENT_ID'),
            'ror_id': UpdatedDataTransformer._extract_identifier(es_doc, 'ROR_ID'),
            'scopus_afid': UpdatedDataTransformer._extract_identifier(es_doc, 'SCOPUS_AFID'),
            
            # Status flags
            'is_active': es_doc.get('IsActive', False),
            'is_replaced_by_id': es_doc.get('IsReplacedById'),
            'has_identifiers': es_doc.get('HasIdentifiers', False),
            'needs_attention': es_doc.get('NeedsAttention', False),
            
            # Parent organization references
            'active_organization_parent_ids': es_doc.get('ActiveOrganizationParentIds', []),
            
            # Metadata
            'created_at': es_doc.get('CreatedAt'),
            'updated_at': es_doc.get('UpdatedAt'),
            'created_by': es_doc.get('CreatedBy'),
            'updated_by': es_doc.get('UpdatedBy'),
            'validated_by': es_doc.get('ValidatedBy'),
            'validated_date': es_doc.get('ValidatedDate'),
            'deleted_at': es_doc.get('DeletedAt'),
            'deleted_by': es_doc.get('DeletedBy'),
            
            # Counts
            'identifiers_count': es_doc.get('IdentifiersCount', 0),
        }
    
    @staticmethod
    def transform_project(es_doc: Dict, es_id: str = None) -> Dict:
        """Transform project document from ES to Neo4j format"""
        doc_id = es_id or es_doc.get('_id') or es_doc.get('Id')
        
        return {
            # Primary key: ES _id
            'es_id': doc_id,
            
            # Display properties for visualization
            'title': es_doc.get('Title'),
            'abstract': es_doc.get('Abstract'),
            
            # Project details
            'start_date': es_doc.get('StartDate'),
            'end_date': es_doc.get('EndDate'),
            'funding_amount': es_doc.get('FundingAmount'),
            'currency': es_doc.get('Currency'),
            'status': es_doc.get('Status'),
            
            # Metadata
            'created_at': es_doc.get('CreatedAt'),
            'updated_at': es_doc.get('UpdatedAt'),
            'created_by': es_doc.get('CreatedBy'),
            'updated_by': es_doc.get('UpdatedBy'),
        }
    
    @staticmethod
    def transform_serial(es_doc: Dict, es_id: str = None) -> Dict:
        """Transform serial document from ES to Neo4j format"""
        doc_id = es_id or es_doc.get('_id') or es_doc.get('Id')
        
        return {
            # Primary key: ES _id
            'es_id': doc_id,
            
            # Display properties for visualization
            'title': es_doc.get('Title'),
            'publisher': es_doc.get('Publisher'),
            
            # Serial details
            'country': es_doc.get('Country'),
            'start_year': es_doc.get('StartYear'),
            'end_year': es_doc.get('EndYear'),
            
            # Identifiers
            'issn': UpdatedDataTransformer._extract_identifier(es_doc, 'ISSN'),
            'eissn': UpdatedDataTransformer._extract_identifier(es_doc, 'EISSN'),
            'scopus_source_id': UpdatedDataTransformer._extract_identifier(es_doc, 'SCOPUS_SOURCE_ID'),
            
            # Classification
            'serial_type': UpdatedDataTransformer._extract_nested_field(es_doc, 'Type', 'DescriptionEng'),
            'serial_type_value': UpdatedDataTransformer._extract_nested_field(es_doc, 'Type', 'Value'),
            
            # Status flags
            'is_open_access': es_doc.get('IsOpenAccess', False),
            'is_peer_reviewed': es_doc.get('IsPeerReviewed', False),
            'is_deleted': es_doc.get('IsDeleted', False),
            
            # Metadata
            'created_date': es_doc.get('CreatedDate'),
            'updated_date': es_doc.get('UpdatedDate'),
            'created_by': es_doc.get('CreatedBy'),
            'updated_by': es_doc.get('UpdatedBy'),
        }
    
    # Helper methods for extracting nested data
    @staticmethod
    def _extract_identifier(doc: Dict, identifier_type: str) -> Optional[str]:
        """Extract specific identifier from identifiers array"""
        identifiers = doc.get('Identifiers', [])
        for ident in identifiers:
            if isinstance(ident, dict):
                type_info = ident.get('Type', {})
                if isinstance(type_info, dict) and type_info.get('Value') == identifier_type:
                    return ident.get('Value')
        return None
    
    @staticmethod
    def _extract_first_or_identifier(doc: Dict, field_name: str, identifier_type: str) -> Optional[str]:
        """Extract from direct field first, then fall back to identifiers array"""
        # Try direct field first (e.g., IdentifierOrcid)
        direct_value = doc.get(field_name)
        if direct_value:
            if isinstance(direct_value, list) and direct_value:
                return direct_value[0]
            elif isinstance(direct_value, str):
                return direct_value
        
        # Fall back to identifiers array
        return UpdatedDataTransformer._extract_identifier(doc, identifier_type)
    
    @staticmethod
    def _extract_nested_field(doc: Dict, parent_field: str, child_field: str) -> Optional[str]:
        """Extract nested field value"""
        parent = doc.get(parent_field, {})
        if isinstance(parent, dict):
            return parent.get(child_field)
        return None
    
    @staticmethod
    def _extract_first_org_type(doc: Dict) -> Optional[str]:
        """Extract first organization type name"""
        org_types = doc.get('OrganizationTypes', [])
        if org_types and isinstance(org_types, list):
            first_type = org_types[0]
            if isinstance(first_type, dict):
                return first_type.get('NameEng')
        return None
    
    @staticmethod
    def _extract_first_org_type_id(doc: Dict) -> Optional[str]:
        """Extract first organization type ID"""
        org_types = doc.get('OrganizationTypes', [])
        if org_types and isinstance(org_types, list):
            first_type = org_types[0]
            if isinstance(first_type, dict):
                return first_type.get('Id')
        return None

class UpdatedRelationshipExtractor:
    """Extract relationships from Elasticsearch documents with ES ID references"""
    
    @staticmethod
    def extract_authorship_relationships(publication_docs: List[Dict]) -> List[Dict]:
        """Extract AUTHORED relationships with ES ID references"""
        relationships = []
        
        for pub_doc in publication_docs:
            pub_es_id = pub_doc.get('_id') or pub_doc.get('Id')
            persons = pub_doc.get('Persons', [])
            
            for i, person in enumerate(persons):
                person_data = person.get('PersonData', {})
                person_es_id = person_data.get('Id')
                
                if person_es_id and pub_es_id:
                    relationships.append({
                        'person_es_id': person_es_id,
                        'publication_es_id': pub_es_id,
                        'order': i + 1,
                        'created_at': datetime.now().isoformat()
                    })
        
        return relationships
    
    @staticmethod
    def extract_affiliation_relationships(person_docs: List[Dict]) -> List[Dict]:
        """Extract AFFILIATED_WITH relationships with ES ID references"""
        relationships = []
        
        for person_doc in person_docs:
            person_es_id = person_doc.get('_id') or person_doc.get('Id')
            org_homes = person_doc.get('OrganizationHome', [])
            
            for org_home in org_homes:
                org_data = org_home.get('OrganizationData', {})
                org_es_id = org_data.get('Id')
                
                if person_es_id and org_es_id:
                    relationships.append({
                        'person_es_id': person_es_id,
                        'organization_es_id': org_es_id,
                        'start_date': org_home.get('StartDate'),
                        'end_date': org_home.get('EndDate'),
                        'title_eng': org_home.get('TitleEng'),
                        'title_swe': org_home.get('TitleSwe'),
                        'priority': org_home.get('Priority'),
                        'source': org_home.get('Source'),
                        'created_at': datetime.now().isoformat()
                    })
        
        return relationships
    
    @staticmethod
    def extract_organization_hierarchy(organization_docs: List[Dict]) -> List[Dict]:
        """Extract PART_OF relationships for organizational hierarchy"""
        relationships = []
        
        for org_doc in organization_docs:
            child_es_id = org_doc.get('_id') or org_doc.get('Id')
            org_parents = org_doc.get('OrganizationParents', [])
            
            for parent_rel in org_parents:
                parent_es_id = parent_rel.get('ParentOrganizationId')
                
                if child_es_id and parent_es_id:
                    relationships.append({
                        'child_es_id': child_es_id,
                        'parent_es_id': parent_es_id,
                        'from_date': parent_rel.get('FromDate'),
                        'to_date': parent_rel.get('ToDate'),
                        'created_at': datetime.now().isoformat()
                    })
        
        return relationships

def main():
    """Test the updated transformers with sample data"""
    print("🧪 Testing Updated Data Transformers")
    
    # Sample Elasticsearch document structure
    sample_person_doc = {
        '_id': 'person-12345-abcde',
        'Id': 'person-12345-abcde',  # Some docs might have both
        'DisplayName': 'Dr. Anna Andersson',
        'FirstName': 'Anna',
        'LastName': 'Andersson',
        'BirthYear': 1980,
        'IsActive': True,
        'HasPublications': True,
        'IdentifierOrcid': ['0000-0002-3456-7890'],
        'Identifiers': [
            {
                'Type': {'Value': 'SCOPUS_AUTHID'},
                'Value': '12345678900'
            }
        ],
        'CreatedAt': '2023-01-01T10:00:00Z'
    }
    
    sample_publication_doc = {
        '_id': 'publication-67890-fghij',
        'Title': 'Advanced Machine Learning Techniques',
        'Abstract': 'This paper explores advanced ML techniques...',
        'Year': 2024,
        'PublicationType': {'NameEng': 'Journal article', 'Id': 'journal-type-1'},
        'IdentifierDoi': ['10.1000/sample.2024.001'],
        'IsDeleted': False
    }
    
    # Test transformations
    print("\n1. Testing Person transformation:")
    person_result = UpdatedDataTransformer.transform_person(sample_person_doc)
    print(f"   ES ID: {person_result['es_id']}")
    print(f"   Display Name: {person_result['display_name']}")
    print(f"   ORCID: {person_result['orcid']}")
    print(f"   Scopus Author ID: {person_result['scopus_author_id']}")
    
    print("\n2. Testing Publication transformation:")
    pub_result = UpdatedDataTransformer.transform_publication(sample_publication_doc)
    print(f"   ES ID: {pub_result['es_id']}")
    print(f"   Title: {pub_result['title']}")
    print(f"   DOI: {pub_result['doi']}")
    print(f"   Publication Type: {pub_result['publication_type']}")
    
    print("\n✅ Data transformer tests completed!")
    print("\n💡 Key Features:")
    print("  - ES _id mapped to es_id property (unique constraint)")
    print("  - Display names preserved for visualization")
    print("  - Robust identifier extraction from multiple fields")
    print("  - Comprehensive field mapping from ES documents")

if __name__ == "__main__":
    main()