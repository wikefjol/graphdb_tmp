# Neo4j Knowledge Graph Schema for Scientific Publications

## Node Types

### Publication
Scientific publication entity (article, thesis, conference paper, etc.)

**Properties:**
```
REQUIRED:
- id: String (unique identifier from source system)
- title: String
- year: Integer
- publicationType: String

OPTIONAL:
- abstract: String
- language: String (ISO code)
- doi: String
- scopusId: String
- pubmedId: String
- isbn: String
- detailsUrlEng: String
- detailsUrlSwe: String
- text: String (concatenated: title + abstract + keywords)
- embedding: Float[] (vector for similarity search)
```

### Person
Researcher/author entity

**Properties:**
```
REQUIRED:
- id: String (unique identifier)
- displayName: String

OPTIONAL:
- firstName: String
- lastName: String
- birthYear: Integer
- orcid: String
- scopusAuthorId: String
- email: String
- cid: String (institutional identifier)
- embedding: Float[] (aggregated from publications)
```

### Organization
Institutional entity (universities, departments, companies)

**Properties:**
```
REQUIRED:
- id: String (unique identifier)
- nameEng: String
- organizationType: String

OPTIONAL:
- nameSwe: String
- displayNameEng: String
- displayNameSwe: String
- displayPathEng: String
- displayPathSwe: String
- city: String
- country: String
- geoLat: Float
- geoLong: Float
- level: Integer (hierarchy level)
- startYear: Integer
- endYear: Integer
```

### Keyword
Research topic/keyword entity

**Properties:**
```
REQUIRED:
- value: String (normalized, lowercase)

OPTIONAL:
- id: String
- embedding: Float[]
```

### Project
Research project entity (optional)

**Properties:**
```
REQUIRED:
- id: String
- title: String

OPTIONAL:
- description: String
- startDate: Date
- endDate: Date
- fundingAmount: Float
```

### Dataset
Research dataset entity (optional)

**Properties:**
```
REQUIRED:
- id: String
- title: String

OPTIONAL:
- description: String
- url: String
```

## Relationships

### Explicit Relationships (from source data)

#### AUTHORED
- **From:** Person
- **To:** Publication
- **Properties:**
  - order: Integer (0-based author position)
  - role: String (default: "Author")

#### CURRENTLY_AFFILIATED_WITH
- **From:** Person
- **To:** Organization
- **Properties:**
  - title: String (position/title)

#### HAS_KEYWORD
- **From:** Publication
- **To:** Keyword
- **Properties:** None

#### PART_OF
- **From:** Organization
- **To:** Organization
- **Properties:** None

#### CITES
- **From:** Publication
- **To:** Publication
- **Properties:** None

### Computed Relationships (generated post-import)

#### COLLABORATES_WITH
- **From:** Person
- **To:** Person
- **Direction:** Undirected (single edge, query both ways)
- **Creation Rule:** Exists if persons co-authored ≥1 publication
- **Properties:**
  - publicationCount: Integer

#### HAS_PUBLISHED_ON
- **From:** Person
- **To:** Keyword
- **Creation Rule:** Person authored publication(s) with keyword
- **Properties:**
  - count: Integer
  - firstYear: Integer
  - lastYear: Integer

## Implementation Instructions

### Constraints
```cypher
CREATE CONSTRAINT publication_id IF NOT EXISTS ON (p:Publication) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT person_id IF NOT EXISTS ON (p:Person) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT organization_id IF NOT EXISTS ON (o:Organization) ASSERT o.id IS UNIQUE;
CREATE CONSTRAINT keyword_value IF NOT EXISTS ON (k:Keyword) ASSERT k.value IS UNIQUE;
```

### Indexes
```cypher
CREATE INDEX publication_year IF NOT EXISTS FOR (p:Publication) ON (p.year);
CREATE INDEX person_orcid IF NOT EXISTS FOR (p:Person) ON (p.orcid);
CREATE INDEX publication_doi IF NOT EXISTS FOR (p:Publication) ON (p.doi);
CREATE VECTOR INDEX publication_embeddings IF NOT EXISTS FOR (p:Publication) ON p.embedding;
CREATE VECTOR INDEX person_embeddings IF NOT EXISTS FOR (p:Person) ON p.embedding;
CREATE VECTOR INDEX keyword_embeddings IF NOT EXISTS FOR (k:Keyword) ON k.embedding;
```

### Data Processing Rules

1. **Keyword Normalization:**
   - Convert to lowercase
   - Trim whitespace
   - Deduplicate on normalized value

2. **Temporal Data:**
   - Skip affiliations with endDate year ≥ 9999
   - Skip affiliations with duration < 6 months
   - Use CURRENTLY_AFFILIATED_WITH for active affiliations

3. **Text Concatenation for Embeddings:**
   - Publication.text = title + " " + abstract + " " + keywords.join(" ")
   - Only create if abstract exists

4. **Person Deduplication:**
   - Primary: ORCID if available
   - Secondary: scopusAuthorId
   - Tertiary: (firstName, lastName, birthYear) tuple

### Relationship Generation Queries

```cypher
-- Generate COLLABORATES_WITH
MATCH (p1:Person)-[:AUTHORED]->(pub:Publication)<-[:AUTHORED]-(p2:Person)
WHERE id(p1) < id(p2)
WITH p1, p2, count(DISTINCT pub) as pubCount
CREATE (p1)-[:COLLABORATES_WITH {publicationCount: pubCount}]->(p2);

-- Generate HAS_PUBLISHED_ON
MATCH (person:Person)-[:AUTHORED]->(pub:Publication)-[:HAS_KEYWORD]->(keyword:Keyword)
WITH person, keyword, 
     count(DISTINCT pub) as pubCount,
     min(pub.year) as firstYear, 
     max(pub.year) as lastYear
CREATE (person)-[:HAS_PUBLISHED_ON {
  count: pubCount, 
  firstYear: firstYear, 
  lastYear: lastYear
}]->(keyword);
```

### GraphRAG Optimization Notes

1. **Embeddings:** Generate after initial import, store as node properties
2. **Vector Indexes:** Create after embeddings are populated
3. **Computed Relationships:** Generate after all explicit relationships exist
4. **Query Patterns:** Use vector search for similarity, graph traversal for context