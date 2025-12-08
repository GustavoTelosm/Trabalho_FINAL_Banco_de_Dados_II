# =============================================================================
# IMPORTAÇÃO DAS BIBLIOTECAS
# =============================================================================
import redis            # Importa a biblioteca para conectar no Redis 
import psycopg2         # Importa a biblioteca para conectar no PostgreSQL 
from neo4j import GraphDatabase # Importa a classe para conectar no Neo4j 
import sys              # Importa funções do sistema 

# =============================================================================
# CONFIGURAÇÕES GERAIS 
# =============================================================================
# Aqui definimos as senhas em variáveis para não precisar digitar toda hora
SENHA_NEO4J = "12345678"  # Senha definida na instalação do Neo4j
SENHA_POSTGRES = "5525"   # Senha definida na instalação do PostgreSQL

# =============================================================================
# MÓDULO NEO4J 
# =============================================================================
def menu_neo4j():
    # Endereço do servidor local do Neo4j (protocolo bolt, porta 7687)
    uri = "bolt://localhost:7687"
    driver = None # Inicializa a variável do driver como vazia

    try:
        # Tenta criar a conexão  usando usuário 'neo4j' e a senha configurada
        driver = GraphDatabase.driver(uri, auth=("neo4j", SENHA_NEO4J))
        
        # Inicia um loop infinito para manter o menu aberto até o usuário decidir voltar
        while True:
            # Exibe o cabeçalho do menu
            print("\n" + "="*40)
            print("   GERENCIADOR NEO4J (Pessoas)")
            print("="*40)
            # Mostra as opções disponíveis (CRUD)
            print("1. [C]REATE  - Criar nova Pessoa")
            print("2. [R]EAD    - Buscar Pessoa por Nome")
            print("3. [U]PDATE  - Atualizar Idade")
            print("4. [D]ELETE  - Remover Pessoa")
            print("0. VOLTAR") # Opção para sair desse menu
            
            # Pede para o usuário digitar o número da opção desejada
            opcao = input("\nEscolha uma operação: ")

            # Abre uma sessão (transação) com o banco de dados
            with driver.session() as session:
                
                #  OPÇÃO 1: CRIAR 
                if opcao == '1':
                    nome = input("Digitar Nome: ")   # Pede o nome
                    idade = input("Digitar Idade: ") # Pede a idade
                    
                    # Executa o comando Cypher para criar um Nó (:Pessoa) com as propriedades
                    session.run("CREATE (p:Pessoa {nome: $nome, idade: $idade})", nome=nome, idade=idade)
                    print(f"✅ SUCESSO: Nó (Nome: {nome}, Idade: {idade}) criado no Grafo.")

                #  OPÇÃO 2: LER 
                elif opcao == '2':
                    nome = input("Qual nome você quer buscar? ") # Pede o nome para busca
                    
                    # Executa o comando Cypher MATCH para encontrar o nó e retornar os dados
                    result = session.run("MATCH (p:Pessoa {nome: $nome}) RETURN p.nome, p.idade", nome=nome)
                    
                    encontrado = False # Variável de controle para saber se achou algo
                    print(f"\n--- Resultado da Busca ---")
                    
                    # O 'result' pode ter vários registros, percorremos eles com um loop
                    for record in result:
                        # Mostra na tela os dados encontrados
                        print(f"👤 Nome: {record['p.nome']} | 🎂 Idade: {record['p.idade']}")
                        encontrado = True # Marca que encontrou pelo menos um
                    
                    # Se não encontrou ninguém, avisa o usuário
                    if not encontrado:
                        print("❌ Ninguém encontrado com esse nome.")

                # OPÇÃO 3: ATUALIZAR (UPDATE) 
                elif opcao == '3':
                    nome = input("Nome da pessoa para atualizar: ") 
                    nova_idade = input("Nova idade: ")             
                    
                    # Executa o comando Cypher SET para mudar a propriedade idade
                    session.run("MATCH (p:Pessoa {nome: $nome}) SET p.idade = $id", nome=nome, id=nova_idade)
                    print(f"✅ SUCESSO: Idade de '{nome}' atualizada para {nova_idade}.")

                # OPÇÃO 4: DELETAR 
                elif opcao == '4':
                    nome = input("Nome da pessoa para DELETAR: ") # Quem vamos apagar?
                    
                    # Executa o comando Cypher DETACH DELETE (apaga o nó e seus relacionamentos)
                    session.run("MATCH (p:Pessoa {nome: $nome}) DETACH DELETE p", nome=nome)
                    print(f"✅ SUCESSO: Nó '{nome}' removido do banco.")

                # SAIR 
                elif opcao == '0':
                    break # Quebra o loop while e volta para o menu principal
                else:
                    print("Opção inválida.") # Caso digite algo que não seja 0, 1, 2, 3 ou 4

    except Exception as e:
        # Se der qualquer erro (conexão, senha, query), mostra aqui
        print(f"🚨 ERRO NO NEO4J: {e}")
    finally:
        # Garante que a conexão seja fechada ao sair, para liberar memória
        if driver: driver.close()


# MÓDULO REDIS 

def menu_redis():
    try:
        # Tenta conectar no Redis local (localhost), porta padrão 6379, banco índice 0
        r = redis.Redis(host='localhost', port=6379, db=0)
        
        while True: # Loop do menu
            print("\n" + "="*40)
            print("   GERENCIADOR REDIS (Cache)")
            print("="*40)
            print("1. [C]REATE  - Criar/Setar Chave")
            print("2. [R]EAD    - Ler Valor da Chave")
            print("3. [U]PDATE  - Atualizar Valor")
            print("4. [D]ELETE  - Apagar Chave")
            print("0. VOLTAR")
            
            opcao = input("\nEscolha uma operação: ")

            # --- OPÇÃO 1 e 3: CRIAR/ATUALIZAR 
            # No Redis, criar e atualizar é o mesmo comando (SET), se a chave já existe, ele sobrescreve
            if opcao == '1' or opcao == '3': 
                chave = input("Digite a CHAVE (ex: aluno:1): ") # Pede o nome da chave
                valor = input("Digite o VALOR: ")               # Pede o conteúdo
                r.set(chave, valor)                             # Salva no Redis
                print(f"✅ SUCESSO: {chave} -> {valor} salvo na memória.")

            # --- OPÇÃO 2: LER 
            elif opcao == '2':
                chave = input("Qual CHAVE você quer ler? ") # Pede a chave
                valor = r.get(chave)                        # Busca o valor no Redis
                
                if valor:
                    # Se achou, precisamos usar .decode('utf-8') porque o Redis retorna em bytes
                    print(f"📦 Valor recuperado: {valor.decode('utf-8')}")
                else:
                    print("❌ Chave não existe.") # Se retornou None 

            # --- OPÇÃO 4: DELETAR 
            elif opcao == '4':
                chave = input("Qual CHAVE você quer apagar? ")
                r.delete(chave) # Apaga a chave da memória
                print(f"✅ SUCESSO: Chave '{chave}' removida.")

            # SAIR 
            elif opcao == '0':
                break
            else:
                print("Opção inválida.")

    except Exception as e:
        print(f"🚨 ERRO NO REDIS: {e}")

# =============================================================================
# MÓDULO POSTGRESQL 
# =============================================================================
def menu_postgres():
    conn = None # Inicializa a variável de conexão vazia
    try:
        # Tenta conectar no banco 'postgres'  com usuário 'postgres' e sua senha
        # 'options' força UTF8 para corrigir bug de acentos no terminal do Windows
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password=SENHA_POSTGRES,
            options="-c client_encoding=UTF8"
        )
        cursor = conn.cursor() # Cria o cursor (a "caneta" que escreve os comandos SQL)
        
        # Cria a tabela automaticamente se ela não existir (para não dar erro na primeira vez)
        cursor.execute("CREATE TABLE IF NOT EXISTS alunos_crud (id SERIAL PRIMARY KEY, nome VARCHAR(100));")
        conn.commit() # Salva a criação da tabela (obrigatório em SQL)

        while True: # Loop do menu
            print("\n" + "="*40)
            print("   GERENCIADOR POSTGRESQL (Tabela)")
            print("="*40)
            print("1. [C]REATE  - Inserir Aluno")
            print("2. [R]EAD    - Listar Todos")
            print("3. [U]PDATE  - Atualizar Nome por ID")
            print("4. [D]ELETE  - Deletar por ID")
            print("0. VOLTAR")
            
            opcao = input("\nEscolha uma operação: ")

            #  OPÇÃO 1: INSERIR 
            if opcao == '1':
                nome = input("Digite o Nome do Aluno: ")
                # Executa o INSERT. O %s é substituído pelo valor da variável 'nome' com segurança
                cursor.execute("INSERT INTO alunos_crud (nome) VALUES (%s)", (nome,))
                conn.commit() # Salva a alteração no banco
                print(f"✅ SUCESSO: '{nome}' inserido na tabela.")

            # OPÇÃO 2: CONSULTAR 
            elif opcao == '2':
                # Busca todos os registros ordenados pelo ID
                cursor.execute("SELECT * FROM alunos_crud ORDER BY id ASC;")
                registros = cursor.fetchall() # Pega todos os resultados e guarda na lista 'registros'
                
                print("\n--- Lista de Alunos ---")
                print("ID | NOME")
                print("---|-----")
                # Loop para imprimir cada linha da tabela
                for reg in registros:
                    print(f"{reg[0]}  | {reg[1]}") # reg[0] é o ID, reg[1] é o Nome
                
                if not registros: # Se a lista estiver vazia
                    print("(Tabela vazia)")

            #  OPÇÃO 3: ATUALIZAR 
            elif opcao == '3':
                id_aluno = input("Digite o ID do aluno para editar: ")
                novo_nome = input("Novo Nome: ")
                # Atualiza o nome onde o ID for igual ao digitado
                cursor.execute("UPDATE alunos_crud SET nome = %s WHERE id = %s", (novo_nome, id_aluno))
                conn.commit() # Salva
                print("✅ SUCESSO: Registro atualizado.")

            #OPÇÃO 4: EXCLUIR 
            elif opcao == '4':
                id_aluno = input("Digite o ID do aluno para EXCLUIR: ")
                # Deleta a linha onde o ID for igual ao digitado
                cursor.execute("DELETE FROM alunos_crud WHERE id = %s", (id_aluno,))
                conn.commit() # Salva
                print("✅ SUCESSO: Registro deletado.")

            # SAIR 
            elif opcao == '0':
                break
            else:
                print("Opção inválida.")
        
        # Fecha o cursor e a conexão ao sair do loop
        cursor.close()
        conn.close()

    except Exception as e:
        # repr(e) mostra o erro técnico completo se algo falhar
        print(f"🚨 ERRO NO POSTGRES: {repr(e)}")

# =============================================================================
# MENU PRINCIPAL 
# =============================================================================
def main():
    while True: # Loop principal do programa
        print("\n" + "█"*40)
        print(" SISTEMA UNIFICADO DE BANCO DE DADOS ")
        print("█"*40)
        print("Onde você deseja operar agora?")
        print("1. Neo4j (Grafos)")
        print("2. Redis (Chave-Valor)")
        print("3. PostgreSQL (Relacional)")
        print("0. SAIR DO SISTEMA")
        
        escolha = input("\n>>> Digite sua escolha: ")

        # Direciona para a função específica dependendo da escolha
        if escolha == '1':
            menu_neo4j() # Vai para o menu do Neo4j
        elif escolha == '2':
            menu_redis() # Vai para o menu do Redis
        elif escolha == '3':
            menu_postgres() # Vai para o menu do PostgreSQL
        elif escolha == '0':
            print("Saindo... Até logo!")
            break 
        else:
            print("Opção inválida!")

# Verifica se o arquivo está sendo executado diretamente (não importado)
if __name__ == "__main__":
    main() # Inicia a função principal