import pandas as pd
import json
import os
import glob
from tqdm import tqdm

# --- CONFIGURAÇÕES ---
PASTA_CSV = 'raw_csv' # Onde estão os CSVs do TSE descompactados
PASTA_SAIDA = 'data'
TOP_CANDIDATOS = 10 

# Mapeamento Manual de Sigla -> Código IBGE (Para garantir que o mapa pinte certo)
UF_TO_IBGE = {
    'AC': '12', 'AL': '27', 'AM': '13', 'AP': '16', 'BA': '29', 'CE': '23', 'DF': '53',
    'ES': '32', 'GO': '52', 'MA': '21', 'MG': '31', 'MS': '50', 'MT': '51', 'PA': '15',
    'PB': '25', 'PE': '26', 'PI': '22', 'PR': '41', 'RJ': '33', 'RN': '24', 'RO': '11',
    'RR': '14', 'RS': '43', 'SC': '42', 'SE': '28', 'SP': '35', 'TO': '17'
}

if not os.path.exists(PASTA_SAIDA):
    os.makedirs(PASTA_SAIDA)

def identificar_colunas(arquivo_csv):
    """Detecta nomes das colunas (TSE muda nomes dependendo do ano)"""
    df_head = pd.read_csv(arquivo_csv, sep=';', encoding='latin1', nrows=0)
    cols = [c.upper() for c in df_head.columns]
    mapa = {}
    
    # Mapeamento flexível
    for c in cols:
        if c in ['QT_VOTOS', 'QTD_VOTOS']: mapa['VOTOS'] = c
        if c in ['NM_URNA_CANDIDATO', 'NM_CANDIDATO']: mapa['CANDIDATO'] = c
        if c in ['SG_PARTIDO']: mapa['PARTIDO'] = c
        if c in ['CD_MUNICIPIO']: mapa['MUNICIPIO'] = c
        if c in ['DS_CARGO']: mapa['CARGO'] = c
        if c in ['SG_UF']: mapa['UF'] = c # Importante para separar por estado
    
    return mapa if len(mapa) >= 6 else None

def processar_ano(ano):
    print(f"🔄 Processando {ano}...")
    arquivos = glob.glob(os.path.join(PASTA_CSV, f"*{ano}*.csv"))
    
    dados_por_cargo = {} # { 'PRESIDENTE': { 'BR': {...}, 'SP': {...}, 'MUNS': {...} } }

    for arq in arquivos:
        cols_map = identificar_colunas(arq)
        if not cols_map: continue
        
        # Carrega colunas essenciais
        usecols = list(cols_map.values())
        
        # Lê em chunks para não estourar memória
        for chunk in pd.read_csv(arq, sep=';', encoding='latin1', usecols=usecols, chunksize=100000, on_bad_lines='skip'):
            # Padroniza nomes
            chunk.rename(columns={v: k for k, v in cols_map.items()}, inplace=True)
            chunk['CARGO'] = chunk['CARGO'].astype(str).str.upper()
            
            # Filtra cargos relevantes
            cargos_interesse = ['PRESIDENTE', 'GOVERNADOR', 'SENADOR']
            chunk = chunk[chunk['CARGO'].isin(cargos_interesse)]
            if chunk.empty: continue

            # Cria ID Único do Candidato
            chunk['ID_CAND'] = chunk['CANDIDATO'] + ' (' + chunk['PARTIDO'] + ')'

            for cargo in cargos_interesse:
                df_cargo = chunk[chunk['CARGO'] == cargo]
                if df_cargo.empty: continue
                
                if cargo not in dados_por_cargo:
                    dados_por_cargo[cargo] = {'ESTADOS': {}, 'MUNICIPIOS': {}}

                # --- AGREGAÇÃO 1: POR ESTADO (Para o mapa inicial) ---
                # Agrupa por UF e Candidato neste chunk
                agrupado_uf = df_cargo.groupby(['UF', 'ID_CAND'])['VOTOS'].sum().reset_index()
                for _, row in agrupado_uf.iterrows():
                    uf_ibge = UF_TO_IBGE.get(row['UF'], row['UF']) # Converte Sigla para Código IBGE (ex: SP -> 35)
                    
                    if uf_ibge not in dados_por_cargo[cargo]['ESTADOS']:
                        dados_por_cargo[cargo]['ESTADOS'][uf_ibge] = {}
                    
                    if row['ID_CAND'] not in dados_por_cargo[cargo]['ESTADOS'][uf_ibge]:
                        dados_por_cargo[cargo]['ESTADOS'][uf_ibge][row['ID_CAND']] = 0
                    
                    dados_por_cargo[cargo]['ESTADOS'][uf_ibge][row['ID_CAND']] += int(row['VOTOS'])

                # --- AGREGAÇÃO 2: POR MUNICÍPIO (Para o drill-down) ---
                # Nota: O TSE usa código de 5 dígitos. O mapa usa 7. 
                # O Frontend tentará lidar com isso, mas o ideal seria converter aqui.
                # Vamos salvar usando o código do TSE mesmo por enquanto.
                agrupado_mun = df_cargo.groupby(['MUNICIPIO', 'ID_CAND'])['VOTOS'].sum().reset_index()
                for _, row in agrupado_mun.iterrows():
                    cod_mun = str(row['MUNICIPIO'])
                    if cod_mun not in dados_por_cargo[cargo]['MUNICIPIOS']:
                        dados_por_cargo[cargo]['MUNICIPIOS'][cod_mun] = {}
                    
                    if row['ID_CAND'] not in dados_por_cargo[cargo]['MUNICIPIOS'][cod_mun]:
                        dados_por_cargo[cargo]['MUNICIPIOS'][cod_mun][row['ID_CAND']] = 0
                        
                    dados_por_cargo[cargo]['MUNICIPIOS'][cod_mun][row['ID_CAND']] += int(row['VOTOS'])

    # --- SALVAR ARQUIVOS OTIMIZADOS ---
    slugs = {'PRESIDENTE': 'president', 'GOVERNADOR': 'governor', 'SENADOR': 'senator'}
    
    for cargo, dados in dados_por_cargo.items():
        # Limpeza e Top N para reduzir tamanho
        # 1. Limpa Estados
        final_estados = dados['ESTADOS'] # Mantém todos os candidatos estaduais
        
        # 2. Limpa Municípios (Aqui reduzimos drasticamente)
        final_muns = {}
        for cod, votos_dict in dados['MUNICIPIOS'].items():
            # Ordena e pega Top N
            sorted_votos = sorted(votos_dict.items(), key=lambda item: item[1], reverse=True)
            top = dict(sorted_votos[:TOP_CANDIDATOS])
            
            # Soma outros
            resto = sum(v for k, v in sorted_votos[TOP_CANDIDATOS:])
            if resto > 0: top['Outros (OUTROS)'] = resto
            
            final_muns[cod] = top

        # Monta JSON final
        payload = {
            "meta": {"ano": ano, "cargo": cargo},
            "estados": final_estados,
            "municipios": final_muns
        }

        nome_arq = f"{ano}_{slugs[cargo]}.json"
        with open(os.path.join(PASTA_SAIDA, nome_arq), 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        print(f"✅ Salvo: {nome_arq}")

# RODAR
processar_ano(2022)
# processar_ano(2018) ...
