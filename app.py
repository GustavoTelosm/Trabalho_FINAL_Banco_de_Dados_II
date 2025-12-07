from neo4j import GraphDatabase

# Configurações de conexão
# O endereço é localhost na porta 7687 (padrão do protocolo Bolt)
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "12345678")

def verificar_conexao():
    driver = None
    try:
        # Tenta criar o driver
        driver = GraphDatabase.driver(URI, auth=AUTH)
        # Verifica se o banco responde
        driver.verify_connectivity()
        print("-" * 30)
        print("SUCESSO! O Python conectou no Neo4j.")
        print("-" * 30)
    except Exception as e:
        print(f"ERRO: Não foi possível conectar. Detalhes: {e}")
    finally:
        if driver:
            driver.close()

if __name__ == "__main__":
    verificar_conexao()