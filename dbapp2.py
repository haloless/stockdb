"""Flask entry for stockdb2 application."""

from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from stockdb2 import init_db
from stockdb2.dbinsert import import_uploaded_files
from stockdb2.dbstat import get_meta, query_stats, query_timeseries

BASE_DIR = Path(__file__).resolve().parent
STATIC_STOCK2 = BASE_DIR / "static" / "stock2"

app = Flask(__name__, static_folder="static")


@app.before_first_request
def _bootstrap() -> None:
    init_db()


@app.route("/")
def root_page():
    return send_from_directory(str(STATIC_STOCK2), "index.html")


@app.route("/stock2/<path:filename>")
def stock2_pages(filename: str):
    return send_from_directory(str(STATIC_STOCK2), filename)


@app.route("/api2/health")
def api_health():
    return jsonify({"ok": True})


@app.route("/api2/meta")
def api_meta():
    return jsonify(get_meta())


@app.route("/api2/upload", methods=["POST"])
def api_upload():
    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "请至少上传一个文件"}), 400

    result = import_uploaded_files(files)
    status_code = 200 if result.get("ok") else 207
    return jsonify(result), status_code


@app.route("/api2/stats")
def api_stats():
    return jsonify(query_stats(request.args))


@app.route("/api2/timeseries")
def api_timeseries():
    return jsonify(query_timeseries(request.args))


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=5000, debug=False)
