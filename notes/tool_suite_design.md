# Chalmers Research Database Tool Suite Design

## User Query Analysis

### Query Categories

1. **Basic Search Queries**
   - "Find publications about machine learning"
   - "Who are the researchers working on sustainability?"
   - "What projects are related to AI?"

2. **Entity-Specific Queries**
   - "Show me details about researcher John Doe"
   - "What publications has Stockholm University contributed to?"
   - "Tell me about the WASP project"

3. **Relationship Queries**
   - "Who collaborates with Chalmers on AI research?"
   - "What organizations participate in energy projects?"
   - "Find co-authors of specific publications"

4. **Analytical Queries**
   - "How many publications per year in computer science?"
   - "Which are the top research areas at Chalmers?"
   - "What's the collaboration network for a specific researcher?"

5. **Temporal Queries**
   - "Recent publications in the last 2 years"
   - "Show research trends over time"
   - "When was this project active?"

6. **Cross-Index Queries**
   - "Find all data related to a specific person"
   - "Show complete project information including participants"
   - "Map publications to their journals"

## Tool Suite Architecture

### Core Search Tools
1. **TextSearchTool** - Full-text search across all indices
2. **PublicationSearchTool** - Specialized publication queries
3. **PersonSearchTool** - Researcher and author searches
4. **OrganizationSearchTool** - Institution and company queries
5. **ProjectSearchTool** - Research project searches
6. **SerialSearchTool** - Journal and publication venue searches

### Advanced Tools
7. **RelationshipTool** - Find connections between entities
8. **AggregationTool** - Statistical analysis and trends
9. **TemporalTool** - Time-based queries and trends
10. **CrossIndexTool** - Multi-index queries and joins
11. **IdentifierTool** - PID-based lookups (DOI, ORCID, etc.)
12. **SimilarityTool** - Find similar entities or content

### Utility Tools
13. **MetadataTool** - Index stats and schema information
14. **ValidationTool** - Data quality and completeness checks

## Tool Interface Design

### Standard Tool Pattern
```python
class BaseTool:
    def search(self, query: str, **kwargs) -> SearchResult
    def get_by_id(self, id: str) -> Optional[dict]
    def count(self, query: str) -> int
    def suggest(self, partial: str) -> List[str]
```

### Return Format
```python
@dataclass
class SearchResult:
    hits: List[dict]
    total: int
    took: int
    max_score: float
    aggregations: Optional[dict] = None
    suggestions: Optional[List[str]] = None
```

## Implementation Plan

### Phase 1: Core Tools (TDD)
- Text search across all indices
- Entity-specific searches
- Basic aggregations

### Phase 2: Advanced Tools
- Relationship mapping
- Temporal analysis
- Cross-index queries

### Phase 3: Utility Tools
- Metadata and validation
- Performance optimization

### Testing Strategy
- Unit tests for each tool
- Integration tests with real data
- Performance benchmarks
- Error handling validation