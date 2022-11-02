from flask.cli import FlaskGroup
from project import app

cli = FlaskGroup(app)


if __name__ == "__main__":
    cli()
    #app.run(host='localhost', port=6000, debug=True)
    #app.run(port=6000, debug=True)