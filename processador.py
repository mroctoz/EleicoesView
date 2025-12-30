import pandas as pd
import json
import os
import glob
import sys

# --- CONFIGURAÇÕES ---
PASTA_CSV = 'raw_csv'   # Coloque os arquivos do TSE aqui
PASTA_SAIDA = 'data'    # Onde os JSONs serão salvos
TOP_CANDIDATOS = 6      # Guarda os 6 mais votados por cidade + Outros

if not os.path.exists(PASTA_SAIDA):
    os.makedirs(PASTA_SAIDA)

def processar(arquivo, ano, cargo_alvo):
    print(f"--> Processando {ano} - {cargo_alvo} em {arquivo}...")
    
    # Colunas comuns do TSE (variam com o tempo, tentaremos várias)
    cols = ['CD_MUNICIPIO', 'NM_URNA_CANDIDATO', 'SG_PARTIDO', 'QT_VOTOS', 'DS_CARGO']
    
    try:
        # Tenta ler CSV padrão TSE (separador ;)
        df = pd.read_csv(arquivo, sep=';', encoding='latin1', usecols=cols, on_bad_lines='skip')
    except ValueError:
        try:
            # Tenta nomes de colunas antigos (antes de 2014)
            cols_antigas = ['CODIGO_MUNICIPIO', 'NOME_URNA_CANDIDATO', 'SIGLA_PARTIDO', 'QTD_VOTOS', 'DESCRICAO_CARGO']
            df = pd.read_csv(arquivo, sep=';', encoding='latin1', usecols=cols_antigas, on_bad_lines='skip')
            df.columns = cols # Renomeia para o padrão novo
        except Exception as e:
            print(f"Erro ao ler {arquivo}: {e}")
            return

    # Filtra Cargo
    df['DS_CARGO'] = df['DS_CARGO'].str.upper()
    df = df[df['DS_CARGO'] == cargo_alvo.upper()]
    
    if df.empty:
        print("    (Nenhum voto encontrado para este cargo)")
        return

    # Cria chave "LULA (PT)"
    df['ID_CAND'] = df['NM_URNA_CANDIDATO'] + ' (' + df['SG_PARTIDO'] + ')'

    # Agrupa votos por Município e Candidato
    print("    Agrupando dados...")
    votos = df.groupby(['CD_MUNICIPIO', 'ID_CAND'])['QT_VOTOS'].sum().reset_index()

    # Estrutura Final: { "COD_IBGE": { "CAND A": 100, "CAND B": 50 } }
    dados_finais = {}
    muns = votos['CD_MUNICIPIO'].unique()

    print(f"    Gerando JSON para {len(muns)} municípios...")

    for mun in muns:
        # Pega fatia do dataframe para este município
        df_mun = votos[votos['CD_MUNICIPIO'] == mun]
        
        # Ordena e pega Top N
        df_mun = df_mun.sort_values(by='QT_VOTOS', ascending=False)
        top = df_mun.head(TOP_CANDIDATOS)
        resto = df_mun.iloc[TOP_CANDIDATOS:]
        
        obj_mun = {}
        for _, row in top.iterrows():
            obj_mun[row['ID_CAND']] = int(row['QT_VOTOS'])
            
        if not resto.empty:
            soma = int(resto['QT_VOTOS'].sum())
            if soma > 0:
                obj_mun['Outros (OUTROS)'] = soma
        
        dados_finais[str(mun)] = obj_mun

    # Salva Arquivo
    slug = 'president' if cargo_alvo == 'PRESIDENTE' else ('governor' if cargo_alvo == 'GOVERNADOR' else 'senator')
    path = os.path.join(PASTA_SAIDA, f"{ano}_{slug}.json")
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(dados_finais, f, ensure_ascii=False)
    
    print(f"    SALVO: {path}")

# --- EXECUÇÃO ---
# Varre a pasta raw_csv e tenta adivinhar o ano pelo nome do arquivo
arquivos = glob.glob(os.path.join(PASTA_CSV, '*.csv'))
print(f"Encontrados {len(arquivos)} arquivos CSV na pasta {PASTA_CSV}")

for arq in arquivos:
    # Lógica simples para detectar ano no nome do arquivo (ex: votacao_2022_BR.csv)
    ano = None
    for y in [2022, 2018, 2014, 2010, 2006, 2002, 1998, 1994]:
        if str(y) in arq:
            ano = y
            break
    
    if ano:
        processar(arq, ano, 'PRESIDENTE')
        processar(arq, ano, 'GOVERNADOR')
        processar(arq, ano, 'SENADOR')
    else:
        print(f"Ignorando {arq} (Ano não detectado no nome)")

print("\nConcluído! Agora suba a pasta 'data' e o 'index.html' para o GitHub.")