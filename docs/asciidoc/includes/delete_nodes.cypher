// n10s created nodes must be deleted before updating Graph Config
MATCH (resource:Resource) DETACH DELETE resource;