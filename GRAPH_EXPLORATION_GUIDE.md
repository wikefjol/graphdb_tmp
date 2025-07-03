# Neo4j Knowledge Graph Exploration Guide

This guide will help you manually explore and understand the research knowledge graph, perfect for demonstrating to colleagues and getting hands-on experience.

## Quick Start - Access Your Graph

### Method 1: Neo4j Browser (Recommended for Queries)
1. Go to your Neo4j Aura instance: https://console.neo4j.io/
2. Click "Open" next to your database
3. Use the credentials from your `.env` file
4. Start with: `MATCH (n) RETURN n LIMIT 25`

### Method 2: Neo4j Bloom (Best for Visual Exploration)
1. From your Aura console, click the "Bloom" tab
2. This provides an intuitive visual interface
3. Great for exploring without writing queries

### Method 3: Python Script (Structured Exploration)
```bash
source neo4j_env/bin/activate
python3 04_graph_exploration_guide.py
```

## Graph Structure Overview

Our knowledge graph contains these entity types:

### 🧑‍🔬 **Person (Researchers)**
- **Properties**: name, ORCID, birth year, active status
- **Sample**: Erik Larsson, Anna Andersson, Maria Svensson
- **Relationships**: AUTHORED → Publication, AFFILIATED_WITH → Organization

### 📚 **Publication (Research Papers)**
- **Properties**: title, abstract, year, DOI, publication type
- **Sample**: "Machine Learning Approaches for Network Security"
- **Relationships**: ← AUTHORED by Person

### 🏢 **Organization (Institutions)**
- **Properties**: English/Swedish names, city, country, level, coordinates
- **Sample**: Chalmers → Dept of CSE → Division of Networks
- **Relationships**: PART_OF (hierarchical), ← AFFILIATED_WITH from Person

### 🔬 **Project (Research Projects)**
- **Properties**: title, funding amount, status, dates
- **Sample**: "AI for Sustainable Computing" (2.5M SEK)
- **Relationships**: ← PARTICIPATES_IN from Person

### 🤝 **Collaboration Networks**
- **CO_AUTHORED_WITH**: Derived from shared publications
- Shows research collaboration strength and patterns

## Essential Queries for Manual Exploration

### 1. Get the Big Picture
```cypher
// See everything (limited view)
MATCH (n)-[r]->(m) 
RETURN n, r, m 
LIMIT 25
```

### 2. Explore Researchers
```cypher
// All researchers with their details
MATCH (p:Person)
RETURN p.display_name, p.orcid, p.birth_year
ORDER BY p.display_name
```

```cypher
// Who works where?
MATCH (p:Person)-[a:AFFILIATED_WITH]->(o:Organization)
RETURN p.display_name as Researcher, 
       o.display_name_eng as Organization,
       a.position as Position
```

### 3. Publication Patterns
```cypher
// Publications with their authors
MATCH (pub:Publication)<-[a:AUTHORED]-(p:Person)
WITH pub, collect(p.display_name) as authors
RETURN pub.title, pub.year, authors
ORDER BY pub.year DESC
```

```cypher
// Most prolific authors
MATCH (p:Person)-[:AUTHORED]->(pub:Publication)
RETURN p.display_name, count(pub) as publications
ORDER BY publications DESC
```

### 4. Collaboration Networks
```cypher
// Direct collaborations
MATCH (p1:Person)-[c:CO_AUTHORED_WITH]->(p2:Person)
RETURN p1.display_name, p2.display_name, c.collaboration_count
```

```cypher
// Collaboration through specific publications
MATCH (p1:Person)-[:AUTHORED]->(pub:Publication)<-[:AUTHORED]-(p2:Person)
WHERE p1.display_name < p2.display_name
RETURN p1.display_name, p2.display_name, pub.title
```

### 5. Organizational Structure
```cypher
// Hierarchy visualization
MATCH (child:Organization)-[:PART_OF]->(parent:Organization)
RETURN child.display_name_eng, parent.display_name_eng
```

### 6. Research Projects
```cypher
// Project participation
MATCH (proj:Project)<-[p:PARTICIPATES_IN]-(person:Person)
RETURN proj.title, proj.funding_amount, 
       collect(person.display_name + " (" + p.role + ")") as team
```

### 7. Cross-Organizational Collaboration
```cypher
// Who collaborates across institutions?
MATCH (p1:Person)-[:AFFILIATED_WITH]->(o1:Organization),
      (p2:Person)-[:AFFILIATED_WITH]->(o2:Organization),
      (p1)-[:CO_AUTHORED_WITH]-(p2)
WHERE o1 <> o2
RETURN p1.display_name, o1.display_name_eng, 
       p2.display_name, o2.display_name_eng
```

## Advanced Exploration Techniques

### Path Analysis
```cypher
// How are two researchers connected?
MATCH (p1:Person {display_name: 'Erik Larsson'}), 
      (p2:Person {display_name: 'Johan Nilsson'})
MATCH path = shortestPath((p1)-[*]-(p2))
RETURN path
```

### Network Analysis
```cypher
// Most connected researchers
MATCH (p:Person)
OPTIONAL MATCH (p)-[r]->()
WITH p, count(r) as connections
RETURN p.display_name, connections
ORDER BY connections DESC
```

### Temporal Patterns
```cypher
// Research output over time
MATCH (pub:Publication)
RETURN pub.year, count(pub) as publications
ORDER BY pub.year
```

## Visual Exploration Tips

### In Neo4j Browser:
1. **Use the graph visualization** - click nodes to expand
2. **Try different layouts** - force-directed, hierarchical
3. **Filter by node types** using the legend on the left
4. **Hover over relationships** to see properties
5. **Right-click nodes** for exploration options

### In Neo4j Bloom:
1. **Start with a search** - type "Person" or "Publication"
2. **Use the perspective** (pre-configured views)
3. **Expand from interesting nodes** by double-clicking
4. **Use filters** to focus on specific aspects
5. **Save interesting views** as scenes

## Understanding the Data Model

### Key Relationship Patterns:
- **Authorship**: Person → AUTHORED → Publication
- **Affiliation**: Person → AFFILIATED_WITH → Organization  
- **Hierarchy**: Organization → PART_OF → Organization
- **Collaboration**: Person ↔ CO_AUTHORED_WITH ↔ Person
- **Project Work**: Person → PARTICIPATES_IN → Project

### Data Quality Insights:
- All researchers have ORCID identifiers (when available)
- Publications include abstracts and DOIs
- Organizations have geographic coordinates
- Temporal data includes project dates and publication years

## Common Exploration Scenarios

### 1. "Show me the collaboration network"
```cypher
MATCH (p1:Person)-[:CO_AUTHORED_WITH]->(p2:Person)
RETURN p1, p2
```

### 2. "Who are the key researchers in this area?"
```cypher
MATCH (p:Person)-[:AUTHORED]->(pub:Publication)
RETURN p.display_name, count(pub) as publications,
       collect(pub.title)[0..3] as sample_papers
ORDER BY publications DESC
```

### 3. "What's the organizational structure?"
```cypher
MATCH (o:Organization)
OPTIONAL MATCH (o)-[:PART_OF]->(parent:Organization)
RETURN o.display_name_eng, o.level, parent.display_name_eng
ORDER BY o.level
```

### 4. "Show me interdisciplinary collaborations"
```cypher
MATCH (p1:Person)-[:AFFILIATED_WITH]->(o1:Organization),
      (p2:Person)-[:AFFILIATED_WITH]->(o2:Organization),
      (p1)-[:AUTHORED]->(pub:Publication)<-[:AUTHORED]-(p2)
WHERE o1.display_name_eng <> o2.display_name_eng
RETURN DISTINCT p1.display_name, o1.display_name_eng,
                p2.display_name, o2.display_name_eng, pub.title
```

## Next Steps for Deeper Analysis

1. **Export interesting visualizations** as images for presentations
2. **Try modifying queries** to answer specific questions
3. **Explore temporal patterns** by filtering on years
4. **Look for research clusters** using community detection
5. **Analyze collaboration strength** using relationship properties

## Questions to Explore With Colleagues

1. **Network Structure**: Who are the most connected researchers?
2. **Collaboration Patterns**: Which departments collaborate most?
3. **Research Output**: How has publication volume changed over time?
4. **Interdisciplinary Work**: Where do we see cross-departmental projects?
5. **Career Trajectories**: How do researcher affiliations change?
6. **Project Networks**: Which projects have the most diverse teams?

## Troubleshooting

### If you don't see expected results:
1. Check that data loaded correctly: `MATCH (n) RETURN count(n)`
2. Verify relationship types: `MATCH ()-[r]->() RETURN DISTINCT type(r)`
3. Look at node properties: `MATCH (n:Person) RETURN n LIMIT 1`

### Performance tips:
1. Use `LIMIT` clauses for large result sets
2. Create indexes on frequently queried properties
3. Use `PROFILE` to understand query performance

Remember: This is a **sandbox environment** for learning and experimentation. Feel free to modify queries and explore different aspects of the research network!