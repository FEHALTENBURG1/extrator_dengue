
"""
EXTRATOR SIMPLES — DENGUE API DADOS ABERTOS SAÚDE

Aplicação Streamlit para pessoas sem conhecimento de programação extraírem dados
brutos de dengue diretamente da API:
https://apidadosabertos.saude.gov.br/arboviroses/dengue

Instalação local:
    pip install streamlit pandas requests

Execução local:
    streamlit run app_extrator_dengue_api.py

Para publicar na web:
    1. Suba este arquivo e o requirements.txt em um repositório GitHub.
    2. Acesse https://share.streamlit.io/
    3. Crie um novo app apontando para app_extrator_dengue_api.py.
"""

from __future__ import annotations

import io
import json
import re
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

BASE_URL = "https://apidadosabertos.saude.gov.br"
ENDPOINT_DENGUE = "/arboviroses/dengue"
URL_DENGUE = f"{BASE_URL}{ENDPOINT_DENGUE}"

UF_DF = "53"

UF_NOME = {
    "11": "Rondônia",
    "12": "Acre",
    "13": "Amazonas",
    "14": "Roraima",
    "15": "Pará",
    "16": "Amapá",
    "17": "Tocantins",
    "21": "Maranhão",
    "22": "Piauí",
    "23": "Ceará",
    "24": "Rio Grande do Norte",
    "25": "Paraíba",
    "26": "Pernambuco",
    "27": "Alagoas",
    "28": "Sergipe",
    "29": "Bahia",
    "31": "Minas Gerais",
    "32": "Espírito Santo",
    "33": "Rio de Janeiro",
    "35": "São Paulo",
    "41": "Paraná",
    "42": "Santa Catarina",
    "43": "Rio Grande do Sul",
    "50": "Mato Grosso do Sul",
    "51": "Mato Grosso",
    "52": "Goiás",
    "53": "Distrito Federal",
}

MUNICIPIOS_DF = {"530010"}

MUNICIPIOS_GO_RIDE = {
    "520025", "520031", "520050", "520110", "520525", "520530",
    "520540", "520570", "520690", "521080", "521205", "521250",
    "521390", "521480", "521490", "521760", "522140", "522158",
}

MUNICIPIOS_MG_RIDE = {
    "310920", "310960", "317010",
}

MUNICIPIOS_RIDE = MUNICIPIOS_DF | MUNICIPIOS_GO_RIDE | MUNICIPIOS_MG_RIDE

MUNICIPIO_NOME_RIDE = {
    "530010": "Brasília/DF",
    "520025": "Água Fria de Goiás/GO",
    "520031": "Águas Lindas de Goiás/GO",
    "520050": "Alexânia/GO",
    "520110": "Cabeceiras/GO",
    "520525": "Cidade Ocidental/GO",
    "520530": "Cocalzinho de Goiás/GO",
    "520540": "Corumbá de Goiás/GO",
    "520570": "Cristalina/GO",
    "520690": "Formosa/GO",
    "521080": "Luziânia/GO",
    "521205": "Mimoso de Goiás/GO",
    "521250": "Novo Gama/GO",
    "521390": "Padre Bernardo/GO",
    "521480": "Pirenópolis/GO",
    "521490": "Planaltina/GO",
    "521760": "Santo Antônio do Descoberto/GO",
    "522140": "Valparaíso de Goiás/GO",
    "522158": "Vila Boa/GO",
    "310920": "Buritis/MG",
    "310960": "Cabeceira Grande/MG",
    "317010": "Unaí/MG",
}


# ============================================================
# FUNÇÕES DE APOIO
# ============================================================

def criar_sessao() -> requests.Session:
    sessao = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    sessao.mount("https://", adapter)
    sessao.mount("http://", adapter)

    sessao.headers.update({
        "accept": "application/json",
        "user-agent": "extrator-dengue-api-streamlit/1.0",
    })

    return sessao


def normalizar_resposta(dados: Any) -> list[dict]:
    """
    A estrutura real observada na API de dengue é:
        {"parametros": [ {...}, {...} ]}

    Mantém fallback para outras chaves caso a API mude.
    """
    if isinstance(dados, list):
        return [x for x in dados if isinstance(x, dict)]

    if isinstance(dados, dict):
        for chave in ("parametros", "data", "registros", "items", "results", "records"):
            valor = dados.get(chave)
            if isinstance(valor, list):
                return [x for x in valor if isinstance(x, dict)]

    return []


def normalizar_codigo(valor: Any, tamanho: int | None = None) -> str:
    if valor is None:
        return ""

    s = str(valor).strip()

    if s.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""

    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]

    s = re.sub(r"\D", "", s)

    if tamanho is not None and s:
        s = s.zfill(tamanho)

    return s


def obter_ano(registro: dict) -> str:
    ano = normalizar_codigo(registro.get("nu_ano"))
    if ano:
        return ano

    dt_notific = str(registro.get("dt_notific", "")).strip()
    if re.match(r"^\d{4}", dt_notific):
        return dt_notific[:4]

    arquivo = str(registro.get("arquivo", "")).upper()
    m = re.search(r"DENGBR(\d{2})", arquivo)
    if m:
        return "20" + m.group(1)

    return ""


def obter_uf_notificacao(registro: dict) -> str:
    return normalizar_codigo(registro.get("sg_uf_not"), tamanho=2)


def obter_municipio_residencia(registro: dict) -> str:
    return normalizar_codigo(registro.get("id_mn_resi"), tamanho=6)[:6]


def registro_passa_filtros(
    registro: dict,
    anos: set[str],
    ufs_notificacao: set[str],
    somente_ride: bool,
    municipios_ride: set[str],
    incluir_df_notificador: bool,
) -> bool:
    ano = obter_ano(registro)
    uf_not = obter_uf_notificacao(registro)
    mun_res = obter_municipio_residencia(registro)

    if anos and ano not in anos:
        return False

    if ufs_notificacao and uf_not not in ufs_notificacao:
        return False

    if somente_ride:
        if incluir_df_notificador:
            if not (uf_not == UF_DF or mun_res in municipios_ride):
                return False
        else:
            if mun_res not in municipios_ride:
                return False

    return True


def adicionar_colunas_auxiliares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria colunas úteis para o usuário entender o recorte.
    Preserva todas as colunas originais.
    """
    if df.empty:
        return df

    out = df.copy()

    out["_ano_extraido"] = out.apply(lambda r: obter_ano(r.to_dict()), axis=1)
    out["_uf_notificacao_codigo"] = out.apply(lambda r: obter_uf_notificacao(r.to_dict()), axis=1)
    out["_uf_notificacao_nome"] = out["_uf_notificacao_codigo"].map(UF_NOME).fillna("")
    out["_municipio_residencia_codigo"] = out.apply(lambda r: obter_municipio_residencia(r.to_dict()), axis=1)
    out["_municipio_residencia_ride_nome"] = out["_municipio_residencia_codigo"].map(MUNICIPIO_NOME_RIDE).fillna("")
    out["_flag_df_notificador"] = out["_uf_notificacao_codigo"].eq(UF_DF)
    out["_flag_ride_residencia"] = out["_municipio_residencia_codigo"].isin(MUNICIPIOS_RIDE)

    return out


def dataframe_para_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def dataframe_para_jsonl(df: pd.DataFrame) -> bytes:
    linhas = []
    for registro in df.to_dict(orient="records"):
        linhas.append(json.dumps(registro, ensure_ascii=False, default=str))
    return ("\n".join(linhas) + "\n").encode("utf-8")


# ============================================================
# COLETA DA API
# ============================================================

def extrair_dados_api(
    anos: set[str],
    ufs_notificacao: set[str],
    somente_ride: bool,
    municipios_ride: set[str],
    incluir_df_notificador: bool,
    limit: int,
    offset_inicial: int,
    max_paginas: int,
    sleep: float,
    parar_quando_repetir: bool,
    area_status,
    progress_bar,
) -> tuple[pd.DataFrame, dict]:
    sessao = criar_sessao()

    registros_filtrados: list[dict] = []
    offset = offset_inicial
    pagina = 0
    total_lido = 0
    total_ano = 0
    total_territorio = 0
    assinatura_anterior = None
    repeticoes = 0
    parou_por = ""

    inicio = datetime.now()

    while pagina < max_paginas:
        url_debug = f"{URL_DENGUE}?limit={limit}&offset={offset}"

        try:
            resposta = sessao.get(
                URL_DENGUE,
                params={"limit": limit, "offset": offset},
                timeout=60,
            )
            resposta.raise_for_status()
            dados = resposta.json()

        except requests.exceptions.RequestException as e:
            parou_por = f"Erro HTTP/API no offset {offset}: {e}"
            break
        except ValueError as e:
            parou_por = f"Erro ao ler JSON no offset {offset}: {e}"
            break

        registros = normalizar_resposta(dados)

        if not registros:
            parou_por = "A API retornou página vazia."
            break

        assinatura_atual = json.dumps(registros, sort_keys=True, ensure_ascii=False, default=str)

        if assinatura_atual == assinatura_anterior:
            repeticoes += 1
        else:
            repeticoes = 0

        assinatura_anterior = assinatura_atual

        filtrados_pagina = [
            r for r in registros
            if registro_passa_filtros(
                registro=r,
                anos=anos,
                ufs_notificacao=ufs_notificacao,
                somente_ride=somente_ride,
                municipios_ride=municipios_ride,
                incluir_df_notificador=incluir_df_notificador,
            )
        ]

        registros_filtrados.extend(filtrados_pagina)

        total_lido += len(registros)
        pagina += 1

        if anos:
            total_ano += sum(1 for r in registros if obter_ano(r) in anos)

        if somente_ride:
            if incluir_df_notificador:
                total_territorio += sum(
                    1 for r in registros
                    if obter_uf_notificacao(r) == UF_DF or obter_municipio_residencia(r) in municipios_ride
                )
            else:
                total_territorio += sum(
                    1 for r in registros
                    if obter_municipio_residencia(r) in municipios_ride
                )

        progress_bar.progress(min(pagina / max_paginas, 1.0))

        area_status.info(
            f"Página {pagina:,} | offset {offset:,} | "
            f"recebidos {len(registros):,} | "
            f"filtrados na página {len(filtrados_pagina):,} | "
            f"filtrados acumulados {len(registros_filtrados):,}"
        )

        if parar_quando_repetir and repeticoes >= 3:
            parou_por = (
                "A API repetiu a mesma página em offsets consecutivos. "
                "A extração foi interrompida para evitar loop infinito."
            )
            break

        offset += limit
        time.sleep(sleep)

    if pagina >= max_paginas:
        parou_por = f"Limite de páginas atingido: {max_paginas:,}."

    fim = datetime.now()

    df = pd.DataFrame(registros_filtrados)

    resumo = {
        "inicio": inicio.strftime("%Y-%m-%d %H:%M:%S"),
        "fim": fim.strftime("%Y-%m-%d %H:%M:%S"),
        "duracao_segundos": round((fim - inicio).total_seconds(), 1),
        "endpoint": URL_DENGUE,
        "limit": limit,
        "offset_inicial": offset_inicial,
        "paginas_lidas": pagina,
        "total_registros_lidos_api": total_lido,
        "total_filtrado": len(df),
        "anos": ", ".join(sorted(anos)) if anos else "todos",
        "ufs_notificacao": ", ".join(sorted(ufs_notificacao)) if ufs_notificacao else "todas",
        "filtro_ride": somente_ride,
        "incluir_df_notificador": incluir_df_notificador,
        "parou_por": parou_por,
    }

    return df, resumo


# ============================================================
# INTERFACE STREAMLIT
# ============================================================

st.set_page_config(
    page_title="Extrator de Dados de Dengue",
    page_icon="🦟",
    layout="wide",
)

st.title("🦟 Extrator simples de dados de dengue")
st.caption("Ferramenta para extrair dados brutos de dengue diretamente da API de Dados Abertos do Ministério da Saúde.")

with st.expander("O que esta aplicação faz?", expanded=True):
    st.markdown(
        """
        Esta aplicação consulta a API:

        `https://apidadosabertos.saude.gov.br/arboviroses/dengue`

        A pessoa escolhe filtros simples, clica em **Extrair dados** e baixa o resultado em CSV.
        Não precisa escrever código Python.
        """
    )

st.sidebar.header("Filtros da extração")

anos_input = st.sidebar.multiselect(
    "Ano(s)",
    options=[str(a) for a in range(2020, 2027)],
    default=["2026"],
    help="O filtro é aplicado localmente depois que a página da API é lida.",
)

recorte = st.sidebar.selectbox(
    "Recorte territorial",
    [
        "DF notificador ou RIDE residência",
        "Apenas DF notificador",
        "Apenas RIDE residência",
        "Brasil inteiro",
        "UF(s) específica(s)",
    ],
    index=0,
)

ufs_escolhidas_nomes = []
ufs_escolhidas_codigos: set[str] = set()

if recorte == "UF(s) específica(s)":
    nome_para_codigo = {nome: cod for cod, nome in UF_NOME.items()}
    ufs_escolhidas_nomes = st.sidebar.multiselect(
        "UF de notificação",
        options=sorted(nome_para_codigo.keys()),
        default=["Distrito Federal"],
    )
    ufs_escolhidas_codigos = {nome_para_codigo[n] for n in ufs_escolhidas_nomes}

somente_ride = recorte in {
    "DF notificador ou RIDE residência",
    "Apenas RIDE residência",
}

incluir_df_notificador = recorte in {
    "DF notificador ou RIDE residência",
    "Apenas DF notificador",
}

if recorte == "Apenas DF notificador":
    ufs_escolhidas_codigos = {UF_DF}
    somente_ride = False

if recorte == "Brasil inteiro":
    ufs_escolhidas_codigos = set()
    somente_ride = False
    incluir_df_notificador = False

municipios_nomes = st.sidebar.multiselect(
    "Municípios da RIDE",
    options=[MUNICIPIO_NOME_RIDE[cod] for cod in sorted(MUNICIPIOS_RIDE)],
    default=[MUNICIPIO_NOME_RIDE[cod] for cod in sorted(MUNICIPIOS_RIDE)],
    help="Usado apenas quando o recorte envolve RIDE.",
)

nome_para_cod_mun = {nome: cod for cod, nome in MUNICIPIO_NOME_RIDE.items()}
municipios_ride_escolhidos = {nome_para_cod_mun[n] for n in municipios_nomes}

st.sidebar.header("Configurações avançadas")

with st.sidebar.expander("Paginação da API"):
    limit = st.number_input("Registros por página", min_value=10, max_value=500, value=100, step=10)
    offset_inicial = st.number_input("Offset inicial", min_value=0, value=0, step=100)
    max_paginas = st.number_input("Máximo de páginas", min_value=1, value=5000, step=100)
    sleep = st.number_input("Pausa entre requisições, em segundos", min_value=0.0, value=0.2, step=0.1)
    parar_quando_repetir = st.checkbox("Parar se a API repetir páginas", value=True)

incluir_auxiliares = st.sidebar.checkbox(
    "Incluir colunas auxiliares no download",
    value=True,
    help="Adiciona colunas como ano extraído, UF notificação e flags de DF/RIDE.",
)

st.subheader("Configuração selecionada")

col_cfg1, col_cfg2, col_cfg3 = st.columns(3)
col_cfg1.metric("Ano(s)", ", ".join(anos_input) if anos_input else "Todos")
col_cfg2.metric("Recorte", recorte)
col_cfg3.metric("Municípios RIDE", len(municipios_ride_escolhidos))

st.warning(
    "A API de dengue não necessariamente filtra por ano/UF no servidor. "
    "Por isso, a aplicação lê páginas da API e aplica os filtros localmente. "
    "Extrações grandes podem demorar."
)

if "resultado_df" not in st.session_state:
    st.session_state["resultado_df"] = None
    st.session_state["resumo"] = None

if st.button("Extrair dados", type="primary"):
    status = st.empty()
    progress = st.progress(0)

    df_resultado, resumo = extrair_dados_api(
        anos=set(anos_input),
        ufs_notificacao=ufs_escolhidas_codigos,
        somente_ride=somente_ride,
        municipios_ride=municipios_ride_escolhidos,
        incluir_df_notificador=incluir_df_notificador,
        limit=int(limit),
        offset_inicial=int(offset_inicial),
        max_paginas=int(max_paginas),
        sleep=float(sleep),
        parar_quando_repetir=parar_quando_repetir,
        area_status=status,
        progress_bar=progress,
    )

    if incluir_auxiliares and not df_resultado.empty:
        df_resultado = adicionar_colunas_auxiliares(df_resultado)

    st.session_state["resultado_df"] = df_resultado
    st.session_state["resumo"] = resumo

    progress.empty()
    status.success("Extração finalizada.")

df_resultado = st.session_state.get("resultado_df")
resumo = st.session_state.get("resumo")

if df_resultado is not None:
    st.divider()
    st.subheader("Resultado da extração")

    if df_resultado.empty:
        st.error("Nenhum registro foi encontrado com os filtros selecionados.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros filtrados", f"{len(df_resultado):,}")
        c2.metric("Colunas", f"{df_resultado.shape[1]:,}")
        c3.metric("Páginas lidas", f"{resumo.get('paginas_lidas', 0):,}")
        c4.metric("Registros lidos da API", f"{resumo.get('total_registros_lidos_api', 0):,}")

        if resumo and resumo.get("parou_por"):
            st.info(f"Parada da extração: {resumo['parou_por']}")

        with st.expander("Resumo técnico da extração", expanded=False):
            st.json(resumo)

        st.dataframe(df_resultado.head(1000), use_container_width=True, height=500)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_nome = f"dengue_api_extraido_{ts}.csv"
        jsonl_nome = f"dengue_api_extraido_{ts}.jsonl"

        col_down1, col_down2 = st.columns(2)

        with col_down1:
            st.download_button(
                "Baixar CSV",
                data=dataframe_para_csv(df_resultado),
                file_name=csv_nome,
                mime="text/csv",
            )

        with col_down2:
            st.download_button(
                "Baixar JSONL",
                data=dataframe_para_jsonl(df_resultado),
                file_name=jsonl_nome,
                mime="application/json",
            )

st.divider()
st.caption(
    "Aplicação desenvolvida para facilitar o acesso aos dados de dengue por pessoas sem programação. "
    "A extração depende da disponibilidade e do comportamento da API pública."
)
