from flask import Blueprint, render_template
from flask_cors import cross_origin

app = Blueprint('pages', __name__)


@app.route('/')
@cross_origin()
def home():
    return render_template('home.html')


@app.route('/documentation')
@cross_origin()
def documentation():
    return render_template('documentation.html')


@app.route('/license')
@cross_origin()
def license():
    return render_template('license.html')


@app.route('/publications')
@cross_origin()
def publications():
    return render_template('publications.html')


@app.route('/about')
@cross_origin()
def about():
    return render_template('about.html')


@app.route('/resources')
@cross_origin()
def resources():
    return render_template('resources.html')
