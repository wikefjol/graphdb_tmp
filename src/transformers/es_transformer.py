#!/usr/bin/env python3
"""
ES Data Transformer
Transform Elasticsearch source documents to Neo4j format with clean architecture
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from ..models.person import PersonCreate
from ..models.publication import PublicationCreate
from ..models.organization import OrganizationCreate


class ESTransformer:
    """Transform real ES source documents to validated Neo4j node format"""
    
    @staticmethod
    def transform_person(es_doc: Dict, es_id: str = None) -> PersonCreate:
        """Transform person document from real ES _source format
        
        Args:
            es_doc: ES document _source data
            es_id: ES document ID (optional)
            
        Returns:
            Validated PersonCreate instance
        """
        
        # Extract ES ID - use provided es_id or Id field from _source
        doc_id = es_id or es_doc.get('Id') or es_doc.get('_es_id')
        if not doc_id:
            raise ValueError("No valid ES ID found for person document")
        
        # Core person fields
        display_name = es_doc.get('DisplayName', '').strip()
        if not display_name:
            raise ValueError("DisplayName is required for person")
        
        # Extract names
        first_name = es_doc.get('FirstName', '').strip()
        last_name = es_doc.get('LastName', '').strip()
        
        # Birth year with validation
        birth_year = es_doc.get('BirthYear')
        if birth_year and not isinstance(birth_year, int):
            try:
                birth_year = int(birth_year)
            except (ValueError, TypeError):
                birth_year = None
        
        # Email extraction (optional)
        email = None  # ES docs typically don't have email in current format
        
        # ORCID extraction and normalization
        orcid = None
        orcid_list = es_doc.get('IdentifierOrcid', [])
        if orcid_list and isinstance(orcid_list, list):
            orcid = str(orcid_list[0]).strip()
        
        # Scopus Author ID
        scopus_author_id = None
        # Extract from complex Identifiers array
        identifiers = es_doc.get('Identifiers', [])
        for identifier in identifiers:
            if isinstance(identifier, dict) and identifier.get('IsActive', True):
                id_type = identifier.get('Type', {})
                if isinstance(id_type, dict):
                    type_value = id_type.get('Value', '')
                    if type_value == 'RESEARCHER_ID':
                        scopus_author_id = identifier.get('Value', '')
                        break
        
        # Institutional ID (CID)
        cid = None
        cpl_ids = es_doc.get('IdentifierCplPersonId', [])
        if cpl_ids and isinstance(cpl_ids, list):
            cid = str(cpl_ids[0])
        
        # Create validated person
        person_data = {
            'id': str(doc_id),
            'displayName': display_name
        }
        
        # Add optional fields only if they have values
        if first_name:
            person_data['firstName'] = first_name
        if last_name:
            person_data['lastName'] = last_name
        if birth_year:
            person_data['birthYear'] = birth_year
        if email:
            person_data['email'] = email
        if orcid:
            person_data['orcid'] = orcid
        if scopus_author_id:
            person_data['scopusAuthorId'] = scopus_author_id
        if cid:
            person_data['cid'] = cid
        
        return PersonCreate(**person_data)
    
    @staticmethod
    def transform_publication(es_doc: Dict, es_id: str = None) -> PublicationCreate:
        """Transform publication document from real ES _source format
        
        Args:
            es_doc: ES document _source data
            es_id: ES document ID (optional)
            
        Returns:
            Validated PublicationCreate instance
        """
        
        # Extract ES ID
        doc_id = es_id or es_doc.get('Id') or es_doc.get('_es_id')
        if not doc_id:
            raise ValueError("No valid ES ID found for publication document")
        
        # Core required fields
        title = es_doc.get('Title', '').strip()
        if not title:
            raise ValueError("Title is required for publication")
        
        year = es_doc.get('Year')
        if not year:
            raise ValueError("Year is required for publication")
        
        # Ensure year is integer
        if not isinstance(year, int):
            try:
                year = int(year)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid year format: {year}")
        
        # Publication type extraction
        publication_type = "article"  # default
        pub_type = es_doc.get('PublicationType', {})
        if isinstance(pub_type, dict):
            type_name = pub_type.get('NameEng', '').lower()
            if type_name:
                # Map common types
                if 'book' in type_name:
                    publication_type = 'book'
                elif 'conference' in type_name or 'proceeding' in type_name:
                    publication_type = 'conference'
                elif 'thesis' in type_name or 'dissertation' in type_name:
                    publication_type = 'thesis'
                else:
                    publication_type = 'article'
        
        # Optional fields
        abstract = es_doc.get('Abstract', '').strip()
        
        # Language extraction
        language = 'en'  # default
        lang_obj = es_doc.get('Language', {})
        if isinstance(lang_obj, dict):
            iso = lang_obj.get('Iso', '').lower()
            if iso in ['en', 'sv', 'de', 'fr', 'es']:
                language = iso
        
        # DOI extraction
        doi = None
        doi_list = es_doc.get('IdentifierDoi', [])
        if doi_list and isinstance(doi_list, list):
            doi = str(doi_list[0]).strip()
        
        # Scopus ID
        scopus_id = None
        scopus_list = es_doc.get('IdentifierScopusId', [])
        if scopus_list and isinstance(scopus_list, list):
            scopus_id = str(scopus_list[0]).strip()
        
        # PubMed ID
        pubmed_id = None
        pubmed_list = es_doc.get('IdentifierPubmedId', [])
        if pubmed_list and isinstance(pubmed_list, list):
            pubmed_id = str(pubmed_list[0]).strip()
        
        # ISBN
        isbn = None
        isbn_list = es_doc.get('IdentifierIsbn', [])
        if isbn_list and isinstance(isbn_list, list):
            isbn = str(isbn_list[0]).strip()
        
        # Journal information
        journal_title = None
        journal_publisher = None
        source = es_doc.get('Source', {})
        if isinstance(source, dict):
            source_serial = source.get('SourceSerial', {})
            if isinstance(source_serial, dict):
                journal_title = source_serial.get('Title', '').strip() or None
                journal_publisher = source_serial.get('Publisher', '').strip() or None
        
        # Details URL
        details_url_eng = es_doc.get('DetailsUrlEng', '').strip() or None
        
        # Create text concatenation for embeddings
        text_parts = [title]
        if abstract:
            text_parts.append(abstract)
        
        # Add keywords to text
        keywords = es_doc.get('Keywords', [])
        if isinstance(keywords, list):
            keyword_strings = []
            for kw in keywords:
                if isinstance(kw, str):
                    keyword_strings.append(kw)
                elif isinstance(kw, dict) and 'Value' in kw:
                    keyword_strings.append(str(kw['Value']))
            if keyword_strings:
                text_parts.append(' '.join(keyword_strings))
        
        text = ' '.join(text_parts)
        
        # Create validated publication
        pub_data = {
            'id': str(doc_id),
            'title': title,
            'year': year,
            'publicationType': publication_type,
            'text': text
        }
        
        # Add optional fields only if they have values
        if abstract:
            pub_data['abstract'] = abstract
        if language:
            pub_data['language'] = language
        if doi:
            pub_data['doi'] = doi
        if scopus_id:
            pub_data['scopusId'] = scopus_id
        if pubmed_id:
            pub_data['pubmedId'] = pubmed_id
        if isbn:
            pub_data['isbn'] = isbn
        if journal_title:
            pub_data['journalTitle'] = journal_title
        if journal_publisher:
            pub_data['journalPublisher'] = journal_publisher
        if details_url_eng:
            pub_data['detailsUrlEng'] = details_url_eng
        
        return PublicationCreate(**pub_data)
    
    @staticmethod
    def transform_organization(es_doc: Dict, es_id: str = None) -> OrganizationCreate:
        """Transform organization document from real ES _source format
        
        Args:
            es_doc: ES document _source data
            es_id: ES document ID (optional)
            
        Returns:
            Validated OrganizationCreate instance
        """
        
        # Extract ES ID
        doc_id = es_id or es_doc.get('Id') or es_doc.get('_es_id')
        if not doc_id:
            raise ValueError("No valid ES ID found for organization document")
        
        # Core required field
        name_eng = es_doc.get('NameEng', '') or es_doc.get('DisplayNameEng', '')
        if not name_eng:
            raise ValueError("NameEng is required for organization")
        name_eng = name_eng.strip()
        
        # Optional fields
        name_swe = es_doc.get('NameSwe', '').strip() or None
        display_name_eng = es_doc.get('DisplayNameEng', '').strip() or None
        display_path_eng = es_doc.get('DisplayPathEng', '').strip() or None
        
        # Level mapping
        level = 'department'  # default
        level_num = es_doc.get('Level', 0)
        if isinstance(level_num, int):
            if level_num == 0:
                level = 'university'
            elif level_num == 1:
                level = 'department'
            elif level_num >= 2:
                level = 'unit'
        
        # Organization type
        organization_type = 'academic'  # default
        org_types = es_doc.get('OrganizationTypes', [])
        if isinstance(org_types, list) and org_types:
            type_obj = org_types[0]
            if isinstance(type_obj, dict):
                type_name = type_obj.get('NameEng', '').lower()
                if 'research' in type_name:
                    organization_type = 'research_institute'
                elif 'admin' in type_name:
                    organization_type = 'administrative'
        
        # Geographic data
        city = es_doc.get('City', '').strip() or None
        country = es_doc.get('Country', '').strip() or None
        
        geo_lat = None
        geo_long = None
        if es_doc.get('GeoLat'):
            try:
                geo_lat = float(es_doc['GeoLat'])
            except (ValueError, TypeError):
                pass
        if es_doc.get('GeoLong'):
            try:
                geo_long = float(es_doc['GeoLong'])
            except (ValueError, TypeError):
                pass
        
        # Create validated organization
        org_data = {
            'id': str(doc_id),
            'nameEng': name_eng
        }
        
        # Add optional fields only if they have values
        if name_swe:
            org_data['nameSwe'] = name_swe
        if display_name_eng:
            org_data['displayNameEng'] = display_name_eng
        if display_path_eng:
            org_data['displayPathEng'] = display_path_eng
        if level:
            org_data['level'] = level
        if organization_type:
            org_data['organizationType'] = organization_type
        if city:
            org_data['city'] = city
        if country:
            org_data['country'] = country
        if geo_lat is not None:
            org_data['geoLat'] = geo_lat
        if geo_long is not None:
            org_data['geoLong'] = geo_long
        
        return OrganizationCreate(**org_data)


class ESRelationshipExtractor:
    """Extract relationships from ES source documents with validation"""
    
    @staticmethod
    def extract_authorship_relationships(publications: List[Dict]) -> List[Dict]:
        """Extract AUTHORED relationships from publication documents
        
        Args:
            publications: List of ES publication documents
            
        Returns:
            List of authorship relationship dictionaries
        """
        relationships = []
        
        for pub in publications:
            pub_source = pub.get('_source', pub)  # Handle both ES format and direct source
            pub_es_id = pub.get('_id') or pub_source.get('Id') or pub_source.get('_es_id')
            
            if not pub_es_id:
                continue
            
            # Extract persons from the Persons array
            persons = pub_source.get('Persons', [])
            for i, person_data in enumerate(persons):
                if isinstance(person_data, dict):
                    person_ref = person_data.get('PersonData', {})
                    person_id = person_ref.get('Id')
                    
                    if person_id:
                        relationships.append({
                            'sourceId': str(person_id),
                            'targetId': str(pub_es_id),
                            'order': i,  # 0-based ordering
                            'role': 'Author',
                            'createdAt': datetime.now()
                        })
        
        return relationships
    
    @staticmethod
    def extract_affiliation_relationships(persons: List[Dict]) -> List[Dict]:
        """Extract AFFILIATED_WITH relationships from person documents
        
        Args:
            persons: List of ES person documents
            
        Returns:
            List of affiliation relationship dictionaries
        """
        relationships = []
        
        for person in persons:
            person_source = person.get('_source', person)  # Handle both ES format and direct source
            person_es_id = person.get('_id') or person_source.get('Id') or person_source.get('_es_id')
            
            if not person_es_id:
                continue
            
            # Extract organization home affiliations
            org_home_list = person_source.get('OrganizationHome', [])
            for org_home in org_home_list:
                if isinstance(org_home, dict):
                    org_data = org_home.get('OrganizationData', {})
                    org_id = org_data.get('Id')
                    
                    if org_id:
                        # Create relationship with metadata
                        rel_data = {
                            'sourceId': str(person_es_id),
                            'targetId': str(org_id),
                            'createdAt': datetime.now()
                        }
                        
                        # Add optional metadata
                        if org_home.get('StartDate'):
                            rel_data['startDate'] = org_home['StartDate']
                        if org_home.get('EndDate'):
                            rel_data['endDate'] = org_home['EndDate']
                        if org_home.get('TitleEng'):
                            rel_data['title'] = org_home['TitleEng']
                        if org_home.get('Priority'):
                            rel_data['priority'] = org_home['Priority']
                        
                        relationships.append(rel_data)
        
        return relationships
    
    @staticmethod
    def extract_organization_hierarchy(organizations: List[Dict]) -> List[Dict]:
        """Extract PART_OF relationships from organization documents
        
        Args:
            organizations: List of ES organization documents
            
        Returns:
            List of hierarchy relationship dictionaries
        """
        relationships = []
        
        for org in organizations:
            org_source = org.get('_source', org)  # Handle both ES format and direct source
            org_es_id = org.get('_id') or org_source.get('Id') or org_source.get('_es_id')
            
            if not org_es_id:
                continue
            
            # Extract parent organization relationships
            org_parents = org_source.get('OrganizationParents', [])
            for parent_ref in org_parents:
                if isinstance(parent_ref, dict):
                    parent_id = parent_ref.get('ParentOrganizationId')
                    
                    if parent_id and parent_id != org_es_id:  # Prevent self-references
                        rel_data = {
                            'childId': str(org_es_id),
                            'parentId': str(parent_id),
                            'createdAt': datetime.now()
                        }
                        
                        # Add optional temporal data
                        if parent_ref.get('FromDate'):
                            rel_data['fromDate'] = parent_ref['FromDate']
                        if parent_ref.get('ToDate'):
                            rel_data['toDate'] = parent_ref['ToDate']
                        
                        relationships.append(rel_data)
        
        return relationships


def get_transformer() -> ESTransformer:
    """Get ES transformer instance"""
    return ESTransformer()


def get_relationship_extractor() -> ESRelationshipExtractor:
    """Get ES relationship extractor instance"""
    return ESRelationshipExtractor()