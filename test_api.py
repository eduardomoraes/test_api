from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, Flask API!"

@app.route('/greet/<name>')
def greet(name):
    greeting = f"Hello, {name}!"
    response = jsonify({'message': greeting})
    return response

if __name__ == '__main__':
    app.run(debug=True)
