# CryptoMarketAnalysis
# Trabalho de curso de verão
* Autor: 
Marcelo Bianchi Barata Ribeiro
* Professores: 
Renato Rocha e Flavio Coelho
* Exchange escolhida: 
Kucoin
* Local da exchange: 
Hong Kong
* Principais mercados da exchange: 
Apenas criptomoedas. A Kucoin não trabalha com moedas propriamente ditas, tais como dólar ou euro.
* Mercados utilizados neste trabalho: 
Ether (ETH), Litecoin (LTC) e NEO

Arquivos:
* Base de dados sqlite:
marcelobbribeiro_ohls_cryptos.sqlite
* Notebook com todas as tarefas requisitadas (extração de dados da API, gerenciamento de banco de dados em SQLite, análise exploratória e visualização):
marcelo_bitcoin_project.ipynb

Notas:
Primeiramente o próprio notebook verifica se a base de dados já foi criada na pasta de trabalho. Em caso afirmativo, sugere-se pular a parte de extração da API. Em caso negativo, o código inteiro deve ser rodado.

O notebook foi dividido nas seções abaixo (além de subseções):
1. Captura de dados do API
2. Armazenamento de dados no SQLite
3. Carregamento do SQLite
4. Visualização de dados

Variáveis utilizadas:
* Date
* Open
* High
* Low
* Close
* Volume
* 5m Return
* Market Volume
