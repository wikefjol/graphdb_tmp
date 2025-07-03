#!/usr/bin/env python3
"""
Graph Operations Utilities
Utility functions for selective graph operations, verification, and maintenance
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from ..core.config import Config
from ..core.neo4j_client import Neo4jClient, get_neo4j_client


class GraphOperations:
    """Comprehensive graph operations and utilities"""
    
    def __init__(self, neo4j_client: Optional[Neo4jClient] = None):
        """Initialize graph operations
        
        Args:
            neo4j_client: Optional Neo4j client instance
        """
        if neo4j_client:
            self.client = neo4j_client
        else:
            config = Config()
            self.client = get_neo4j_client(config)
    
    def get_node_counts(self) -> Dict[str, int]:
        """Get counts of all node types in the graph
        
        Returns:
            Dictionary with node type counts
        """
        queries = {
            'organizations': "MATCH (n:Organization) RETURN count(n) as count",
            'persons': "MATCH (n:Person) RETURN count(n) as count",
            'publications': "MATCH (n:Publication) RETURN count(n) as count",
            'keywords': "MATCH (n:Keyword) RETURN count(n) as count"
        }
        
        counts = {}
        for node_type, query in queries.items():
            result = self.client.execute_query(query)
            counts[node_type] = result[0]['count'] if result else 0
        
        return counts
    
    def get_relationship_counts(self) -> Dict[str, int]:
        """Get counts of all relationship types in the graph
        
        Returns:
            Dictionary with relationship type counts
        """
        queries = {
            'part_of': "MATCH ()-[r:PART_OF]->() RETURN count(r) as count",
            'affiliated_with': "MATCH ()-[r:AFFILIATED_WITH]->() RETURN count(r) as count",
            'authored': "MATCH ()-[r:AUTHORED]->() RETURN count(r) as count",
            'has_keyword': "MATCH ()-[r:HAS_KEYWORD]->() RETURN count(r) as count"
        }
        
        counts = {}
        for rel_type, query in queries.items():
            result = self.client.execute_query(query)
            counts[rel_type] = result[0]['count'] if result else 0
        
        return counts
    
    def verify_graph_integrity(self) -> Dict[str, Any]:
        """Verify graph integrity and identify potential issues
        
        Returns:
            Dictionary with integrity check results
        """
        print("🔍 Verifying graph integrity...")
        
        integrity_results = {
            'timestamp': datetime.now().isoformat(),
            'node_counts': self.get_node_counts(),
            'relationship_counts': self.get_relationship_counts(),
            'issues': []
        }
        
        # Check for self-referential PART_OF relationships
        self_ref_query = """
        MATCH (child:Organization)-[r:PART_OF]->(parent:Organization)
        WHERE child.id = parent.id
        RETURN count(r) as self_ref_count
        """
        result = self.client.execute_query(self_ref_query)
        self_ref_count = result[0]['self_ref_count'] if result else 0
        
        if self_ref_count > 0:
            integrity_results['issues'].append(f"Found {self_ref_count} self-referential PART_OF relationships")
        else:
            print("   ✅ No self-referential PART_OF relationships found")
        
        # Check for cycles in organizational hierarchy
        cycle_query = """
        MATCH path = (start:Organization)-[:PART_OF*2..10]->(start)
        RETURN count(path) as cycle_count
        """
        result = self.client.execute_query(cycle_query)
        cycle_count = result[0]['cycle_count'] if result else 0
        
        if cycle_count > 0:
            integrity_results['issues'].append(f"Found {cycle_count} cycles in organizational hierarchy")
        else:
            print("   ✅ No cycles in organizational hierarchy found")
        
        # Check for orphaned nodes (nodes without any relationships)
        orphan_queries = {
            'orphaned_persons': """
                MATCH (p:Person)
                WHERE NOT (p)-[:AUTHORED]->() AND NOT (p)-[:AFFILIATED_WITH]->()
                RETURN count(p) as count
            """,
            'orphaned_publications': """
                MATCH (pub:Publication)
                WHERE NOT ()-[:AUTHORED]->(pub) AND NOT (pub)-[:HAS_KEYWORD]->()
                RETURN count(pub) as count
            """,
            'orphaned_organizations': """
                MATCH (o:Organization)
                WHERE NOT (o)-[:PART_OF]->() AND NOT ()-[:PART_OF]->(o) AND NOT ()-[:AFFILIATED_WITH]->(o)
                RETURN count(o) as count
            """
        }
        
        for orphan_type, query in orphan_queries.items():
            result = self.client.execute_query(query)
            orphan_count = result[0]['count'] if result else 0
            if orphan_count > 0:
                integrity_results['issues'].append(f"Found {orphan_count} {orphan_type}")
            else:
                print(f"   ✅ No {orphan_type} found")
        
        # Check data quality
        quality_checks = {
            'persons_without_display_name': """
                MATCH (p:Person)
                WHERE p.displayName IS NULL OR p.displayName = ''
                RETURN count(p) as count
            """,
            'publications_without_title': """
                MATCH (pub:Publication)
                WHERE pub.title IS NULL OR pub.title = ''
                RETURN count(pub) as count
            """,
            'organizations_without_name': """
                MATCH (o:Organization)
                WHERE o.nameEng IS NULL OR o.nameEng = ''
                RETURN count(o) as count
            """
        }
        
        for check_name, query in quality_checks.items():
            result = self.client.execute_query(query)
            issue_count = result[0]['count'] if result else 0
            if issue_count > 0:
                integrity_results['issues'].append(f"Found {issue_count} {check_name}")
            else:
                print(f"   ✅ No {check_name} found")
        
        if not integrity_results['issues']:
            print("   🎉 Graph integrity verification passed!")
        else:
            print(f"   ⚠️  Found {len(integrity_results['issues'])} integrity issues")
        
        return integrity_results
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get comprehensive graph statistics
        
        Returns:
            Dictionary with detailed graph statistics
        """
        print("📊 Gathering graph statistics...")
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'nodes': self.get_node_counts(),
            'relationships': self.get_relationship_counts()
        }
        
        # Organizational hierarchy statistics
        hierarchy_stats = {}
        
        # Root organizations (no parents)
        root_query = """
        MATCH (o:Organization)
        WHERE NOT (o)-[:PART_OF]->()
        RETURN count(o) as root_count
        """
        result = self.client.execute_query(root_query)
        hierarchy_stats['root_organizations'] = result[0]['root_count'] if result else 0
        
        # Leaf organizations (no children)
        leaf_query = """
        MATCH (o:Organization)
        WHERE NOT ()-[:PART_OF]->(o)
        RETURN count(o) as leaf_count
        """
        result = self.client.execute_query(leaf_query)
        hierarchy_stats['leaf_organizations'] = result[0]['leaf_count'] if result else 0
        
        # Maximum hierarchy depth
        depth_query = """
        MATCH path = (leaf:Organization)-[:PART_OF*]->(root:Organization)
        WHERE NOT (leaf)-[:PART_OF]->() AND NOT ()-[:PART_OF]->(root)
        RETURN max(length(path)) as max_depth
        """
        result = self.client.execute_query(depth_query)
        hierarchy_stats['max_hierarchy_depth'] = result[0]['max_depth'] if result else 0
        
        stats['organizational_hierarchy'] = hierarchy_stats
        
        # Author and publication statistics
        pub_stats = {}
        
        # Publications per year distribution
        year_dist_query = """
        MATCH (pub:Publication)
        WHERE pub.year IS NOT NULL
        RETURN pub.year as year, count(pub) as count
        ORDER BY year DESC
        LIMIT 10
        """
        result = self.client.execute_query(year_dist_query)
        pub_stats['publications_by_year'] = {record['year']: record['count'] for record in result}
        
        # Authors per publication statistics
        author_stats_query = """
        MATCH (pub:Publication)
        OPTIONAL MATCH (p:Person)-[:AUTHORED]->(pub)
        WITH pub, count(p) as author_count
        RETURN 
            min(author_count) as min_authors,
            max(author_count) as max_authors,
            avg(author_count) as avg_authors
        """
        result = self.client.execute_query(author_stats_query)
        if result:
            pub_stats.update(result[0])
        
        stats['publications'] = pub_stats
        
        # Person statistics
        person_stats = {}
        
        # Persons with ORCID
        orcid_query = """
        MATCH (p:Person)
        WHERE p.orcid IS NOT NULL
        RETURN count(p) as orcid_count
        """
        result = self.client.execute_query(orcid_query)
        person_stats['persons_with_orcid'] = result[0]['orcid_count'] if result else 0
        
        # Active affiliations
        active_aff_query = """
        MATCH (p:Person)-[r:AFFILIATED_WITH]->()
        WHERE r.endDate IS NULL
        RETURN count(r) as active_affiliations
        """
        result = self.client.execute_query(active_aff_query)
        person_stats['active_affiliations'] = result[0]['active_affiliations'] if result else 0
        
        stats['persons'] = person_stats
        
        return stats
    
    def clear_graph_selectively(self, entity_types: List[str] = None, relationship_types: List[str] = None) -> Dict[str, int]:
        """Clear specific entity types or relationship types from the graph
        
        Args:
            entity_types: List of entity types to clear (e.g., ['Person', 'Publication'])
            relationship_types: List of relationship types to clear (e.g., ['AUTHORED'])
            
        Returns:
            Dictionary with counts of deleted items
        """
        deleted_counts = {}
        
        # Clear relationships first to avoid constraint violations
        if relationship_types:
            for rel_type in relationship_types:
                query = f"MATCH ()-[r:{rel_type}]->() DELETE r RETURN count(r) as deleted"
                result = self.client.execute_write(query)
                deleted_counts[f'{rel_type}_relationships'] = result.get('deleted', 0)
                print(f"   🗑️  Deleted {deleted_counts[f'{rel_type}_relationships']} {rel_type} relationships")
        
        # Clear nodes
        if entity_types:
            for entity_type in entity_types:
                query = f"MATCH (n:{entity_type}) DELETE n RETURN count(n) as deleted"
                result = self.client.execute_write(query)
                deleted_counts[f'{entity_type}_nodes'] = result.get('deleted', 0)
                print(f"   🗑️  Deleted {deleted_counts[f'{entity_type}_nodes']} {entity_type} nodes")
        
        return deleted_counts
    
    def export_graph_subset(self, entity_types: List[str] = None, limit: int = None) -> Dict[str, List[Dict]]:
        """Export a subset of the graph for analysis or backup
        
        Args:
            entity_types: List of entity types to export
            limit: Maximum number of nodes per type to export
            
        Returns:
            Dictionary with exported data
        """
        export_data = {}
        
        if not entity_types:
            entity_types = ['Organization', 'Person', 'Publication']
        
        for entity_type in entity_types:
            query = f"MATCH (n:{entity_type}) RETURN n"
            if limit:
                query += f" LIMIT {limit}"
            
            result = self.client.execute_query(query)
            export_data[entity_type.lower()] = [record['n'] for record in result]
            print(f"   📤 Exported {len(export_data[entity_type.lower()])} {entity_type} nodes")
        
        return export_data
    
    def close(self):
        """Close Neo4j client connection"""
        self.client.close()


def verify_graph_state() -> Dict[str, Any]:
    """Standalone function to verify graph state
    
    Returns:
        Graph verification results
    """
    ops = GraphOperations()
    try:
        return ops.verify_graph_integrity()
    finally:
        ops.close()


def get_graph_statistics() -> Dict[str, Any]:
    """Standalone function to get graph statistics
    
    Returns:
        Graph statistics
    """
    ops = GraphOperations()
    try:
        return ops.get_graph_statistics()
    finally:
        ops.close()


def clear_graph_completely() -> Dict[str, int]:
    """Standalone function to clear the entire graph
    
    Returns:
        Counts of deleted items
    """
    ops = GraphOperations()
    try:
        print("🗑️  Clearing entire graph...")
        
        # Clear all relationships first
        rel_query = "MATCH ()-[r]->() DELETE r"
        ops.client.execute_write(rel_query)
        
        # Clear all nodes
        node_query = "MATCH (n) DELETE n"
        result = ops.client.execute_write(node_query)
        
        print("   ✅ Graph cleared completely")
        return {'deleted_nodes': result.get('counters', {}).get('nodes_deleted', 0)}
    finally:
        ops.close()


def get_node_counts() -> Dict[str, int]:
    """Standalone function to get node counts
    
    Returns:
        Dictionary with node counts
    """
    ops = GraphOperations()
    try:
        return ops.get_node_counts()
    finally:
        ops.close()


if __name__ == "__main__":
    """CLI interface for graph operations"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python graph_operations.py [verify|stats|counts|clear]")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "verify":
        results = verify_graph_state()
        print(f"\n📊 Graph Verification Results:")
        print(f"   Nodes: {results['node_counts']}")
        print(f"   Relationships: {results['relationship_counts']}")
        if results['issues']:
            print(f"   Issues: {len(results['issues'])}")
            for issue in results['issues']:
                print(f"     - {issue}")
        else:
            print("   ✅ No issues found")
    
    elif command == "stats":
        stats = get_graph_statistics()
        print(f"\n📊 Graph Statistics:")
        print(f"   Nodes: {stats['nodes']}")
        print(f"   Relationships: {stats['relationships']}")
        print(f"   Organizational Hierarchy: {stats['organizational_hierarchy']}")
        print(f"   Publications: {stats.get('publications', {})}")
        print(f"   Persons: {stats.get('persons', {})}")
    
    elif command == "counts":
        counts = get_node_counts()
        print(f"\n📊 Node Counts:")
        for node_type, count in counts.items():
            print(f"   {node_type}: {count:,}")
    
    elif command == "clear":
        confirm = input("⚠️  Are you sure you want to clear the entire graph? (yes/no): ")
        if confirm.lower() == "yes":
            result = clear_graph_completely()
            print(f"   🗑️  Deleted {result['deleted_nodes']} nodes")
        else:
            print("   Operation cancelled")
    
    else:
        print(f"Unknown command: {command}")
        print("Available commands: verify, stats, counts, clear")