from flask import Flask, jsonify
from users import users

app= Flask(__name__)


@app.route("/", methods=["GET"])
def ping():
    return jsonify({"mensaje":"hello world"})

@app.route("/users")
def usersHandler():
    return jsonify({"users":users})


if __name__== "__main__":
    app.run(debug=True, port=4000)