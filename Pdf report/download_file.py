from flask import Flask, send_file

app=Flask(__name__)

@app.route('/download',methods=['GET'])
def download_file():
    p='C:/Users/jatin c/Downloads/generate report/Daily Productivity Report(North East Region).xlsx'
    return send_file(p, as_attachment=True)


if __name__=='__main__':
    app.run(debug=True)