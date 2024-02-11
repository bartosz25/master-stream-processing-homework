#!flask/bin/python
from flask import Flask


app = Flask(__name__.split('.')[0])

if __name__ == '__main__':
    from visits import create_visits_endpoint
    create_visits_endpoint(app)

    app.run(debug=True, port=8088, host='localhost')
