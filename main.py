import os
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "mensagem": "API Kommo Presença online"
    })


@app.route("/consulta", methods=["POST"])
def consulta():
    try:
        data = request.get_json(silent=True) or {}

        lead_id = data.get("lead_id")

        return jsonify({
            "status": "ok",
            "mensagem": "Requisição recebida com sucesso",
            "lead_id_recebido": lead_id
        })

    except Exception as e:
        return jsonify({
            "status": "erro",
            "mensagem": str(e)
        }), 500


if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
