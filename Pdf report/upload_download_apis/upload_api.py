from werkzeug import secure_filename
from flask import Flask, request, render_template,redirect
app=Flask(__name__,template_folder='templates')

'''@app.route("/upload-image", methods=["GET", "POST"])
def upload_image():
    return render_template("public/upload_image.html")'''


'''@app.route('/uploadfile')
def uploader():
    f=request.files['file']
    filename = secure_filename(f.filename)
    #fname=f.filename.split('.')
    UPLOAD_DIRECTORY="G:/VScode workpsace/upload_download_apis/"
    UPLOAD_DIRECTORY +=filename
    f.save(UPLOAD_DIRECTORY)'''

#from flask import request, redirect
import os

app.config["IMAGE_UPLOADS"] = "G:/VScode workspace/upload_download_apis/"

@app.route("/upload-image", methods=["GET", "POST"])
def upload_image():

    if request.method == "POST":

        if request.files:

            image = request.files["image"]

            image.save(os.path.join(app.config["IMAGE_UPLOADS"], image.filename))

            print("Image saved")

            return redirect(request.url)

    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)