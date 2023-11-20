from flask import Blueprint, request, jsonify, url_for, redirect
import traceback
import logging
logger = logging.getLogger("DEBUG")

app = Blueprint('errors', __name__)


@app.errorhandler(404)
def errors_404(e):
    logger.debug(traceback.format_exc())
    if request.path.startswith(f'/api'):
        return jsonify({"error": f'API Endpoint not found: {request.path} -> Check spelling and the API docs'}), 404
    return redirect(url_for('home')), 404, {'Refresh': f'1; url={url_for("home")}'}


@app.errorhandler(ValueError)
def errors_valueerror(e):
    logger.debug(traceback.format_exc())
    return jsonify({"error": str(e)}), 422


@app.errorhandler(AssertionError)
def errors_assertion(e):
    logger.debug(traceback.format_exc())
    return jsonify({"error": "invalid input argument", "code": 100})


@app.errorhandler(Exception)
def errors_general_exception(e: Exception):
    logger.debug(traceback.format_exc())
    return jsonify({"error": f"An unexpected error occurred: {e}", "code": 9999}), 500
