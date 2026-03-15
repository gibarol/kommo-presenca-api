import os
from flask import Flask, request, jsonify

app = Flask(__name__)


# -------------------------------
# Endpoint de status da API
# -------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "mensagem": "API Kommo Presença online"
    })


# -------------------------------
# Endpoint principal de consulta
# -------------------------------
@app.route("/consulta", methods=["GET", "POST"])
def consulta():
    try:

        cpf = None
        lead_id = None

        # ==========================
        # Requisição via navegador
        # ==========================
        if request.method == "GET":
            cpf = request.args.get("cpf")

        # ==========================
        # Requisição via Kommo
        # ==========================
        if request.method == "POST":
            data = request.get_json(silent=True) or {}

            cpf = data.get("cpf")
            lead_id = data.get("lead_id")

        # Validação básica
        if not cpf:
            return jsonify({
                "status": "erro",
                "mensagem": "CPF não informado"
            }), 400

        # =====================================
        # AQUI VAI ENTRAR A CONSULTA DO BANCO
        # =====================================
        # Por enquanto estamos simulando retorno

        resposta = {
            "cpf": cpf,
            "elegivel": True,
            "valor_disponivel": 35000,
            "parcela": 1100
        }

        return jsonify({
            "status": "ok",
            "lead_id": lead_id,
            "resultado": resposta
        })

    except Exception as e:
        return jsonify({
            "status": "erro",
            "mensagem": str(e)
        }), 500


# -------------------------------
# Inicialização da aplicação
# -------------------------------
if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
