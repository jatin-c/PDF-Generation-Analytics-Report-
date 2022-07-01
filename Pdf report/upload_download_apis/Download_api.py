from flask import Flask, send_file

app=Flask(_name_)

@app.route('/download',methods=['GET'])
def download_file():
    p='C:/Users/jatin c/Downloads/generate report/Daily Productivity Report(North East Region).xlsx'
    return send_file(p, as_attachment=True)


if _name=='main_':
    app.run(debug=True)