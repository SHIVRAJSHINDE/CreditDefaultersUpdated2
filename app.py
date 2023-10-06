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
        if 'csv_file' not in request.files:
            return render_template('index.html', message="No file selected.")

        file = request.files['csv_file']

        # Check if the file has a valid name and extension
        if file.filename == '':
            return render_template('index.html', message="No file selected.")
        if not file.filename.endswith('.csv'):
            return render_template('index.html', message="Invalid file format. Please upload a CSV file.")

        try:
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(file)
            print(df)
            pred = PredictPipeline()
            clusters ,finalPred = pred.predit(df)
            print(clusters ,finalPred)

            # You can now work with the DataFrame as needed
            # For example, you can convert it to HTML and pass it to a template
            table_html = df.to_html(classes='table table-bordered table-hover')

            # Render an HTML template with the table
            return render_template('index.html', table=table_html)
        except Exception as e:
            return render_template('index.html', message=f"An error occurred: {str(e)}")


if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)
