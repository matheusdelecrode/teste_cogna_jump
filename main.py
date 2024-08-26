import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time

def process_chunk(chunk):
    produtos_mais_vendidos = {}
    vendas_por_pais_regiao = {}
    vendas_mensais = {}

    for _, row in chunk.iterrows():
        produto = row['Item Type']
        quantidade = row['Units Sold']
        canal = row['Sales Channel']
        pais = row['Country']
        regiao = row['Region']
        data_venda = pd.to_datetime(row['Order Date'])
        receita = row['Total Revenue']

        # Acumula quantidade vendida por produto e canal
        if (produto, canal) not in produtos_mais_vendidos:
            produtos_mais_vendidos[(produto, canal)] = 0
        produtos_mais_vendidos[(produto, canal)] += quantidade

        # Acumula receita total por país e região
        if (pais, regiao) not in vendas_por_pais_regiao:
            vendas_por_pais_regiao[(pais, regiao)] = 0
        vendas_por_pais_regiao[(pais, regiao)] += receita

        # Acumula receita mensal por produto
        mes = data_venda.to_period('M')
        if (produto, mes) not in vendas_mensais:
            vendas_mensais[(produto, mes)] = 0
        vendas_mensais[(produto, mes)] += receita

    return produtos_mais_vendidos, vendas_por_pais_regiao, vendas_mensais

if __name__ == '__main__':
    chunksize = 10 ** 6

    produtos_mais_vendidos_total = {}
    vendas_por_pais_regiao_total = {}
    vendas_mensais_total = {}

    start_time = time.time()  # Início da medição de tempo

    # Cria um executor de processos para realizar processamento paralelo
    with ProcessPoolExecutor() as executor:
        futures = []  # Lista para armazenar as futuras tarefas de processamento
        
        # Processa o arquivo CSV em chunks
        for chunk in pd.read_csv('vendas.csv', chunksize=chunksize):
            chunk.columns = chunk.columns.str.strip()
            
            # Envia cada chunk para processamento paralelo e armazena o futuro correspondente
            future = executor.submit(process_chunk, chunk)
            futures.append(future)

        # Aguarda a conclusão de todas as tarefas e coleta os resultados
        for future in futures:
            # Obtém o resultado do processamento do chunk
            produtos_mais_vendidos, vendas_por_pais_regiao, vendas_mensais = future.result()
            
            # Atualiza o dicionário com os produtos mais vendidos
            for d in produtos_mais_vendidos:
                if d not in produtos_mais_vendidos_total:
                    produtos_mais_vendidos_total[d] = 0
                produtos_mais_vendidos_total[d] += produtos_mais_vendidos[d]
            
            # Atualiza o dicionário com as vendas por país e região
            for d in vendas_por_pais_regiao:
                if d not in vendas_por_pais_regiao_total:
                    vendas_por_pais_regiao_total[d] = 0
                vendas_por_pais_regiao_total[d] += vendas_por_pais_regiao[d]
            
            # Atualiza o dicionário com as vendas mensais
            for d in vendas_mensais:
                if d not in vendas_mensais_total:
                    vendas_mensais_total[d] = 0
                vendas_mensais_total[d] += vendas_mensais[d]

    end_time = time.time()  # Fim da medição de tempo
    elapsed_time_seconds = end_time - start_time
    elapsed_time_minutes = elapsed_time_seconds / 60

    # Produto mais vendido
    produto_mais_vendido = max(produtos_mais_vendidos_total, key=produtos_mais_vendidos_total.get)
    print(f"Produto mais vendido e canal: {produto_mais_vendido} com {produtos_mais_vendidos_total[produto_mais_vendido]} unidades")

    # País e região com maior volume de vendas
    pais_regiao_mais_vendas = max(vendas_por_pais_regiao_total, key=vendas_por_pais_regiao_total.get)
    print(f"País e região com maior volume de vendas: {pais_regiao_mais_vendas} com {vendas_por_pais_regiao_total[pais_regiao_mais_vendas]:.2f}")

    # Cálculo da média de vendas mensais por produto
    vendas_por_produto = {}
    num_meses_por_produto = {}

    for (produto, mes), receita in vendas_mensais_total.items():
        if produto not in vendas_por_produto:
            vendas_por_produto[produto] = 0
            num_meses_por_produto[produto] = 0
        vendas_por_produto[produto] += receita
        num_meses_por_produto[produto] += 1

    media_vendas_mensais = {produto: vendas_por_produto[produto] / num_meses_por_produto[produto] for produto in vendas_por_produto}

    print("Média de vendas mensais por produto:")
    for produto, media in media_vendas_mensais.items():
        print(f"{produto}: {media:.2f}")

    print(f"Tempo total de processamento: {elapsed_time_minutes:.2f} minutos")
