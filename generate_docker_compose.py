#!/usr/bin/env python3
"""
Generate docker-compose.yaml with a configurable number of client containers.
Usage: python generate_docker_compose.py <num_clients>
"""

import sys


def generate_docker_compose(num_clients):
    """Generate docker-compose.yaml content with specified number of clients."""
    
    yaml_content = """services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=server
      - SERVER_PORT=5678

"""
    
    for i in range(num_clients):
        yaml_content += f"""  client_{i}:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_{i}
    depends_on:
      - server
    environment:
      - AGENCY_ID={i}
      - SERVER_HOST=server
      - SERVER_PORT=5678

"""
    
    return yaml_content


def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_docker_compose.py <num_clients>")
        print("Example: python generate_docker_compose.py 5")
        sys.exit(1)
    
    try:
        num_clients = int(sys.argv[1])
        if num_clients < 1:
            print("Error: Number of clients must be at least 1")
            sys.exit(1)
    except ValueError:
        print("Error: Number of clients must be an integer")
        sys.exit(1)
    
    yaml_content = generate_docker_compose(num_clients)
    
    with open("docker-compose.yaml", "w") as f:
        f.write(yaml_content)
    
    print(f"Generated docker-compose.yaml with {num_clients} client containers")


if __name__ == "__main__":
    main()
