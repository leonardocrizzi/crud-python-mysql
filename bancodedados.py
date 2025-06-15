import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd
import time as t

# Iniciaçização do banco de dados
load_dotenv()

conexao = mysql.connector.connect(
    host= os.getenv("HOST_BANCO"),
    user= os.getenv("USER_BANCO"),
    password= os.getenv("SENHA_BANCO"),
    port= int(os.getenv("PORT_BANCO")),
    database= 'crud'
)

cursor = conexao.cursor()

# CRUD
def crud():
    while True:
        print("------------------------------------------------")
        menu = int(input("Excolha uma opção:\n1 - Novo Produto\n2 - Ler produtos\n3 - Atualizar produtos\n4 - Remover produtos\n5 - Sair\nOpção: "))
        print("------------------------------------------------")
        
        if menu == 1:
            tabela_insert = input("Qual o nome da tabela: ")
            qtd_colunas = int(input("Qauntas colunas deseja adicionar valores: "))
            lista_colunas = []

            for i in range (qtd_colunas): 
                coluna_insert = input(f"Digite o nome da {i+1}ª coluna: ")
                lista_colunas.append((coluna_insert))

            valores_colunas = {}

            print("------------------------------------------------")
            print(f"Colunas selecionadas: {", ".join(lista_colunas)}")
            print("------------------------------------------------")

            for i, nome_coluna in enumerate(lista_colunas):
                entrada = input(f"Digite o valor para a coluna '{nome_coluna}': ")
                valores_colunas[nome_coluna] = entrada
            
            df = pd.read_sql(f'SELECT * FROM {tabela_insert} LIMIT 1', con=conexao)
    
            valores_formatados = []

            for coluna in lista_colunas:
                tipo = df.dtypes[coluna]
                entrada = valores_colunas[coluna]

                # Tratamento do valor de acordo com o tipo da coluna
                if pd.api.types.is_numeric_dtype(tipo):
                    valor = entrada  # números podem ser inseridos diretamente
                elif pd.api.types.is_datetime64_any_dtype(tipo):
                    valor = f'"{pd.to_datetime(entrada)}"'  # formato de data
                else:
                    valor = f'"{entrada}"'  # strings entre aspas

                valores_formatados.append(valor)

            comando_ddl = f'INSERT INTO {tabela_insert} ({", ".join(lista_colunas)}) VALUES ({", ".join(valores_formatados)})'
            cursor.execute(comando_ddl)
            conexao.commit()

        elif menu == 2:
            query = 'SELECT * FROM vendas'
            cursor.execute(query)
            resultado = cursor.fetchall()
            print(resultado)

        elif menu == 3:
            df = pd.read_sql('SELECT * FROM vendas', con=conexao)

            tabela = input("Qual tabela gostaria de modificar: ")
            coluna = input("Qual coluna gostaria de modificar: ")
            entrada = input("Qual valor você gostaria de modificar: ")

            tipo_coluna = df[coluna].dtype

            if pd.api.types.is_numeric_dtype(tipo_coluna):
                valor = float(entrada) if 'float' in str(tipo_coluna) else int(entrada)
            elif pd.api.types.is_datetime64_any_dtype(tipo_coluna):
                valor = pd.to_datetime(entrada)
            else:
                valor = f'"{entrada}"'

            coluna_condicao = input("Qual coluna irá na condição: ")
            entrada_condicao = input("Qual valor de condição você gostaria de usar: ")
            tipo_coluna_2 = df[coluna_condicao].dtype

            if pd.api.types.is_numeric_dtype(tipo_coluna_2):
                valor_condicao = float(entrada_condicao) if 'float' in str(tipo_coluna_2) else int(entrada_condicao)
            elif pd.api.types.is_datetime64_any_dtype(tipo_coluna_2):
                valor_condicao = pd.to_datetime(entrada_condicao)
            else:
                valor_condicao = f'"{entrada_condicao}"'

            comando_update = f'UPDATE {tabela} SET {coluna} = {valor} WHERE {coluna_condicao} = {valor_condicao}'
            cursor.execute(comando_update)
            conexao.commit()

        elif menu == 4:
            tabela_delete = input("Qual tabela gostaria de usar: ")
            coluna_delete = input("Qual a coluna que deseja selecionar: ")
            entrada_delete = input("Qual o valor de remoção: ")
            tipo_coluna_delete = df[coluna_delete].dtype

            if pd.api.types.is_numeric_dtype(tipo_coluna_delete):
                valor_delete = float(entrada_delete) if 'float' in str(tipo_coluna_delete) else int(entrada_delete)
            elif pd.api.types.is_datetime64_any_dtype(tipo_coluna_delete):
                valor_delete = pd.to_datetime(entrada_delete)
            else:
                valor_delete = f'"{entrada_delete}"'

            comando_delete = f'DELETE FROM {tabela_delete} WHERE {coluna_delete} = {valor_delete}'
            cursor.execute(comando_delete)
            conexao.commit()

        elif menu == 5:
            print("Saindo do programa...")
            t.sleep(5)
            break

        else:
            print("Opção Inválida! Escolha novamente outra opção!")

crud()
            
cursor.close()
conexao.close()