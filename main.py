import os
import requests
from fastapi import FastAPI
from typing import Any, List

app = FastAPI()

BASE_URL = "https://presenca-bank-api.azurewebsites.net"
LOGIN = os.getenv("PRESENCA_LOGIN")
SENHA = os.getenv("PRESENCA_SENHA")

TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", 45))


# -----------------------------
# LOGIN
# -----------------------------
def presenca_login():

    url = f"{BASE_URL}/login"

    payload = {
        "login": LOGIN,
        "senha": SENHA
    }

    r = requests.post(url, json=payload, timeout=TIMEOUT)
    r.raise_for_status()

    token = r.json().get("token")

    return {
        "Authorization": f"Bearer {token}"
    }


# -----------------------------
# AUTORIZAÇÃO DEVICE
# -----------------------------
def autorizar_device(headers, autorizacao_id):

    url = f"{BASE_URL}/consultas/termo-inss/{autorizacao_id}"

    payload = {
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "operationalSystem": "macOS 10.15.7",
        "deviceModel": "MacBook Pro",
        "deviceName": "MacBook Pro 15\"",
        "deviceType": "desktop",
        "geoLocation": {
            "latitude": "-27.6450",
            "longitude": "-48.6678"
        }
    }

    r = requests.put(url, json=payload, headers=headers, timeout=TIMEOUT)

    return r.json()


# -----------------------------
# VÍNCULOS
# -----------------------------
def consultar_vinculos(headers, cpf):

    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos"

    payload = {
        "cpf": cpf
    }

    r = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)

    r.raise_for_status()

    return r.json()


# -----------------------------
# MARGEM
# -----------------------------
def consultar_margem(headers, cpf, matricula, cnpj):
    import time

    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-margem"

    payload = {
        "cpf": cpf,
        "matricula": matricula,
        "cnpj": cnpj
    }

    time.sleep(2)

    r = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)

    if r.status_code == 429:
        return {
            "erro_rate_limit": True,
            "mensagem": "Limite de requisições atingido no endpoint de margem"
        }

    r.raise_for_status()
    return r.json()


# -----------------------------
# SIMULAÇÃO
# -----------------------------
def simular(headers, cpf, telefone, matricula, cnpj, margem):
    url = f"{BASE_URL}/v5/operacoes/simulacao/disponiveis"

    ddd = telefone[:2] if len(telefone) >= 10 else "11"
    numero = telefone[2:] if len(telefone) >= 10 else "999999999"

    payload = {
        "tomador": {
            "telefone": {
                "ddd": ddd,
                "numero": numero
            },
            "cpf": cpf,
            "nome": margem.get("nome") or "CLIENTE",
            "dataNascimento": margem.get("dataNascimento") or "1982-10-05",
            "nomeMae": margem.get("nomeMae") or "NAO INFORMADO",
            "email": "email@teste.com",
            "sexo": margem.get("sexo") or "M",
            "vinculoEmpregaticio": {
                "cnpjEmpregador": cnpj,
                "registroEmpregaticio": matricula
            },
            "dadosBancarios": {
                "codigoBanco": None,
                "agencia": None,
                "conta": None,
                "digitoConta": None,
                "formaCredito": None
            },
            "endereco": {
                "cep": "",
                "rua": "",
                "numero": "",
                "complemento": "",
                "cidade": "",
                "estado": "",
                "bairro": ""
            }
        },
        "proposta": {
            "valorSolicitado": 0,
            "quantidadeParcelas": 0,
            "produtoId": 28,
            "valorParcela": margem.get("valorMargemDisponivel", 0)
        },
        "documentos": []
    }

    r = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# -----------------------------
# EXTRAIR VÍNCULO
# -----------------------------
def extract_vinculo(body: Any) -> List[dict]:

    if isinstance(body, list):
        return body

    if isinstance(body, dict):

        for key in ["id", "data", "result", "items", "vinculos"]:
            val = body.get(key)

            if isinstance(val, list):
                return val

        return [body]

    return []


# -----------------------------
# CONSULTA PRINCIPAL
# -----------------------------
@app.get("/consulta")
def consulta(cpf: str, nome: str, telefone: str, autorizacao_id: str = None):

    headers = presenca_login()

    # -------------------------
    # SE JÁ TEM AUTORIZAÇÃO
    # -------------------------
    if autorizacao_id:

        autorizar_device(headers, autorizacao_id)

    # -------------------------
    # CONSULTA VÍNCULOS
    # -------------------------
    try:

        vinculos = consultar_vinculos(headers, cpf)

    except Exception as e:

        return {
            "status": "erro",
            "mensagem": str(e)
        }

    lista = extract_vinculo(vinculos)

    if not lista:

        return {
            "status": "erro",
            "mensagem": "Nenhum vínculo encontrado"
        }

    v = lista[0]

    matricula = v.get("matricula")
    cnpj = v.get("numeroInscricaoEmpregador")

    if not matricula or not cnpj:

        return {
            "status": "erro",
            "mensagem": "Não foi possível extrair matrícula/cnpj do vínculo",
            "detalhe_vinculo": v
        }

    # -------------------------
    # MARGEM
    # -------------------------
    margem = consultar_margem(headers, cpf, matricula, cnpj)

    detalhe_margem = margem

    valor_disponivel = detalhe_margem.get("valorMargemDisponivel")

    # -------------------------
    # SIMULAÇÃO
    # -------------------------
    simulacao = simular(headers, cpf, telefone, matricula, cnpj, margem)

    lista_simulacao = simulacao if isinstance(simulacao, list) else []

    if not lista_simulacao:

        return {
            "status": "erro",
            "mensagem": "Simulação vazia"
        }

    melhor = lista_simulacao[0]

    valor_liberado = melhor.get("valorLiberado")
    parcela = melhor.get("valorParcela")

    return {
        "status": "sucesso",
        "cpf": cpf,
        "matricula": matricula,
        "cnpj": cnpj,
        "elegibilidade": "sim",
        "valor_disponivel": valor_liberado,
        "parcela": parcela,
        "detalhe_margem": detalhe_margem,
        "detalhe_simulacao": lista_simulacao,
        "detalhe_vinculo": v
    }


# -----------------------------
# HEALTH CHECK
# -----------------------------
@app.get("/")
def health():

    return {
        "status": "api rodando"
    }
