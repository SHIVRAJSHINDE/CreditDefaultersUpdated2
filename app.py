from flask import Flask, request,render_template
import numpy as np
import pandas as pd
from Source.prediction.predict_pipeline import CustomData,PredictPipeline


application = Flask(__name__)
app=application

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/predictdata',methods=['GET','POST'])
def predict_datapoint():
    if request.method=='GET':
        return render_template('home.html')
    else:
        if "csv_file" not in request.files:
            return "No file part"
        csv_file = request.files["csv_file"]
        # Check if the file has a name
        if csv_file.filename == "":
            return "No selected file"
        # Check if the file is a CSV file
        if not csv_file.filename.endswith(".csv"):
            return "File is not a CSV"
        # Read the content of the CSV file
        csv_content = csv_file.read()
        # You can process the CSV data here (e.g., parse and display)
        return render_template("home.html")
        #return f"File '{csv_file.filename}' uploaded successfully."




if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)
