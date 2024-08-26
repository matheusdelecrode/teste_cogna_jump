# Desafio de Processamento de Dados de Grande Volume

## Problema

Precisamos processar um grande arquivo CSV para calcular estatísticas de vendas. O desafio é fazer isso de maneira eficiente para não sobrecarregar a memória e reduzir o tempo de execução.

## Solução

1. **Divisão do Arquivo em Pedaços**:
   - O arquivo CSV é dividido em pedaços menores, chamados "chunks", com 1 milhão de linhas cada. Isso evita o uso excessivo de memória.

2. **Processamento Paralelo**:
   - Usamos `ProcessPoolExecutor` para processar esses pedaços em paralelo. Isso significa que vários pedaços são processados ao mesmo tempo, o que acelera o processo.

3. **Função de Processamento**:
   - Para cada pedaço, a função `process_chunk` calcula:
     - Quantidade total vendida por produto e canal.
     - Receita total por país e região.
     - Receita mensal por produto.

4. **Consolidação dos Resultados**:
   - Depois de processar todos os pedaços, reunimos os resultados para obter:
     - O produto mais vendido e o canal.
     - O país e a região com maior volume de vendas.
     - A média de vendas mensais por produto.

5. **Medição de Tempo**:
   - Medimos o tempo total de processamento em segundos e depois convertemos para minutos para saber quanto tempo levou.

## Otimizações

- **Paralelização**: Permite que vários processadores trabalhem ao mesmo tempo, tornando o processamento mais rápido.
- **Chunks**: Processar em pedaços menores ajuda a gerenciar melhor a memória.

## Resultados

- **Produto Mais Vendido**: Identificamos o produto que teve mais unidades vendidas e o Canal de vendas.
- **País e Região com Mais Vendas**: Encontramos o país e a região que geraram mais receita.
- **Média de Vendas Mensais**: Calculamos a média de vendas mensais para cada produto.
