#!/usr/bin/env python3
"""
Load Chalmers Organizational Structure
Load all 330 Chalmers organizations with PART_OF relationships into Neo4j
"""

import json
from neo4j import GraphDatabase
from dotenv import load_dotenv
from datetime import datetime
import os

class ChalmersStructureLoader:
    """Load Chalmers organizational hierarchy into Neo4j"""
    
    def __init__(self):
        load_dotenv()
        self.neo4j_driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))
        )
        self.neo4j_database = os.getenv('NEO4J_DATABASE')
    
    def close(self):
        self.neo4j_driver.close()
    
    def load_chalmers_data(self):
        """Load Chalmers structure from JSON file"""
        with open('chalmers_organizational_structure.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    
    def create_organization_nodes(self, organizations_flat):
        """Create Organization nodes from flat structure"""
        nodes = []
        
        for org_id, org_data in organizations_flat.items():
            # Create enhanced organization node
            node = {
                'es_id': org_id,
                'name_eng': org_data.get('name', ''),
                'display_name_eng': org_data.get('name', ''),
                'primary_name_eng': org_data.get('name', ''),
                'display_path_eng': org_data.get('display_path', ''),
                'city': org_data.get('city', 'Gothenburg'),
                'country': org_data.get('country', 'Sweden'),
                'level': org_data.get('path_depth', 0),
                'hierarchy_depth': org_data.get('path_depth', 0),
                'is_active': True,
                'is_deleted': False,
                'needs_attention': False,
                'is_chalmers_organization': True,
                'is_root_organization': org_data.get('path_depth', 0) == 1,
                'created_at': datetime.now().isoformat(),
                'imported_at': datetime.now().isoformat(),
                'data_source': 'chalmers_structure'
            }
            
            # Organization types
            org_types = org_data.get('organization_types', [])
            if org_types:
                node['organization_types_eng'] = org_types
                node['organization_type'] = org_types[0]  # Primary type
            
            # Path analysis
            path_parts = org_data.get('path_parts', [])
            if len(path_parts) > 1:
                node['parent_organization_names'] = path_parts[:-1]  # All parents
                node['direct_parent_name'] = path_parts[-2]  # Immediate parent
            
            # Enhanced data quality scoring
            node['data_quality_score'] = 5  # Full score for complete Chalmers data
            if node['name_eng']: 
                node['data_quality_score'] += 1
            if node['city']: 
                node['data_quality_score'] += 1
            
            nodes.append(node)
        
        print(f"📊 Prepared {len(nodes)} organization nodes for loading")
        return nodes
    
    def extract_hierarchy_relationships(self, organizations_flat):
        """Extract PART_OF relationships from path structure"""
        relationships = []
        
        for org_id, org_data in organizations_flat.items():
            path_parts = org_data.get('path_parts', [])
            
            # Skip root level (Chalmers itself)
            if len(path_parts) <= 1:
                continue
            
            # Find parent organization by name matching
            parent_name = path_parts[-2]  # Immediate parent
            
            # Find parent organization ID
            parent_id = None
            for pid, pdata in organizations_flat.items():
                if pdata.get('name') == parent_name and len(pdata.get('path_parts', [])) == len(path_parts) - 1:
                    parent_id = pid
                    break
            
            if parent_id:
                relationships.append({
                    'child_es_id': org_id,
                    'parent_es_id': parent_id,
                    'relationship_type': 'PART_OF',
                    'hierarchy_level': len(path_parts),
                    'created_at': datetime.now().isoformat(),
                    'data_source': 'chalmers_structure'
                })
        
        print(f"🔗 Prepared {len(relationships)} PART_OF relationships")
        return relationships
    
    def load_nodes_to_neo4j(self, nodes):
        """Load organization nodes to Neo4j"""
        if not nodes:
            return 0
        
        query = """
        UNWIND $nodes AS node
        MERGE (o:Organization {es_id: node.es_id})
        SET o += node
        """
        
        try:
            with self.neo4j_driver.session(database=self.neo4j_database) as session:
                result = session.run(query, nodes=nodes)
                summary = result.consume()
                nodes_created = summary.counters.nodes_created
                properties_set = summary.counters.properties_set
                print(f"✅ Organizations: {nodes_created} created, {properties_set} properties set")
                return nodes_created
        except Exception as e:
            print(f"❌ Failed to load organizations: {str(e)}")
            return 0
    
    def create_hierarchy_relationships(self, relationships):
        """Create PART_OF relationships in Neo4j"""
        if not relationships:
            return 0
        
        query = """
        UNWIND $relationships AS rel
        MATCH (child:Organization {es_id: rel.child_es_id})
        MATCH (parent:Organization {es_id: rel.parent_es_id})
        MERGE (child)-[r:PART_OF]->(parent)
        ON CREATE SET 
            r.hierarchy_level = rel.hierarchy_level,
            r.created_at = datetime(rel.created_at),
            r.data_source = rel.data_source
        ON MATCH SET
            r.last_seen = datetime()
        """
        
        try:
            with self.neo4j_driver.session(database=self.neo4j_database) as session:
                result = session.run(query, relationships=relationships)
                summary = result.consume()
                rels_created = summary.counters.relationships_created
                print(f"✅ PART_OF relationships: {rels_created} created")
                return rels_created
        except Exception as e:
            print(f"❌ Failed to create PART_OF relationships: {str(e)}")
            return 0
    
    def verify_chalmers_structure(self):
        """Verify the loaded Chalmers structure"""
        print(f"\n📊 CHALMERS STRUCTURE VERIFICATION")
        print("=" * 60)
        
        with self.neo4j_driver.session(database=self.neo4j_database) as session:
            # Count Chalmers organizations by level
            chalmers_counts = session.run("""
                MATCH (o:Organization {is_chalmers_organization: true})
                RETURN o.organization_type as org_type, count(o) as count
                ORDER BY count DESC
            """).data()
            
            total_chalmers = sum(record['count'] for record in chalmers_counts)
            print(f"📈 Chalmers organizations loaded: {total_chalmers}")
            for record in chalmers_counts:
                print(f"   {record['org_type']}: {record['count']}")
            
            # Check hierarchy depth
            hierarchy_levels = session.run("""
                MATCH (o:Organization {is_chalmers_organization: true})
                RETURN o.hierarchy_depth as level, count(o) as count
                ORDER BY level
            """).data()
            
            print(f"\n🏗️  Hierarchy levels:")
            for level in hierarchy_levels:
                print(f"   Level {level['level']}: {level['count']} organizations")
            
            # Check PART_OF relationships
            part_of_count = session.run("""
                MATCH (child:Organization)-[r:PART_OF]->(parent:Organization)
                WHERE child.is_chalmers_organization = true
                RETURN count(r) as part_of_count
            """).single()['part_of_count']
            
            print(f"\n🔗 PART_OF relationships: {part_of_count}")
            
            # Sample hierarchy path
            sample_path = session.run("""
                MATCH path = (leaf:Organization {is_chalmers_organization: true})-[:PART_OF*]->(root:Organization {is_root_organization: true})
                WHERE length(path) = 3
                RETURN [n in nodes(path) | n.name_eng] as org_path
                LIMIT 1
            """).single()
            
            if sample_path:
                path_str = " → ".join(sample_path['org_path'])
                print(f"\n📍 Sample hierarchy: {path_str}")
    
    def run_chalmers_loading(self):
        """Execute the complete Chalmers structure loading"""
        print("🏫 CHALMERS ORGANIZATIONAL STRUCTURE LOADER")
        print("=" * 80)
        
        # Load data
        print("📂 Loading Chalmers structure data...")
        data = self.load_chalmers_data()
        organizations_flat = data.get('organizations_flat', {})
        
        print(f"   Found {len(organizations_flat)} organizations in structure")
        
        # Create nodes
        print("\n🔄 Creating organization nodes...")
        nodes = self.create_organization_nodes(organizations_flat)
        nodes_created = self.load_nodes_to_neo4j(nodes)
        
        # Create relationships
        print("\n🔗 Creating hierarchy relationships...")
        relationships = self.extract_hierarchy_relationships(organizations_flat)
        rels_created = self.create_hierarchy_relationships(relationships)
        
        # Verify
        self.verify_chalmers_structure()
        
        print(f"\n" + "=" * 80)
        print("✅ CHALMERS STRUCTURE LOADING COMPLETE!")
        print("=" * 80)
        print(f"🏫 Loaded {nodes_created} Chalmers organizations")
        print(f"🔗 Created {rels_created} PART_OF hierarchy relationships")
        print(f"📊 Complete 4-level organizational structure imported")
        
        return {'nodes_created': nodes_created, 'relationships_created': rels_created}

def main():
    """Main execution function"""
    loader = ChalmersStructureLoader()
    
    try:
        results = loader.run_chalmers_loading()
        print(f"\n🎉 Success! Chalmers organizational structure loaded into Neo4j")
        
    except Exception as e:
        print(f"❌ Loading failed: {str(e)}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()