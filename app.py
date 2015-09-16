#!/usr/bin/env python
import os
import dpark
import string
from flask import *
from pymongo import *
from flask.ext.pymongo import PyMongo
from bson import json_util, objectid
from werkzeug import secure_filename

UPLOAD_FOLDER = '/var/www/flask/uploads'
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
#app.config['MONGO_URL'] = 'mongodb://senz:123456@119.254.111.40:27017/zhiyi_test'
#mongo = PyMongo(app)

#conn = pymongo.MongoClient('127.0.0.1', 27017)
#db = conn.FlaskDB
#db.LogProc = db.LogProc
conn = MongoClient('mongodb://senz:123456@119.254.111.40:27017/zhiyi_test')
#conn.api.authenticate("senz","123456")
db = conn.zhiyi_test


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods = ['POST'])
def upload():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            f = dpark.textFile(os.path.join(UPLOAD_FOLDER, filename))
            chs = f.flatMap(lambda x: x).filter(lambda x: x in string.ascii_letters).map(lambda x: (x, 1))
            wc = chs.reduceByKey(lambda x, y: x+y).collectAsMap()
            db.LogProc.save(wc)
    return redirect('/')

@app.route('/logprocs/v1/gets/', methods=['GET'])
def get_results():
    results = db.LogProc.find().sort('_id')
    #return json_util.dumps(results)
    return render_template('index.html', results=results)

@app.route('/logprocs/v1/gets/<rid>', methods=['GET'])
def get_result(rid):
    result = db.LogProc.find_one(objectid.ObjectId(rid))
    #return json_util.dumps(result)
    return render_template('index.html', result=result)

@app.route('/logprocs/v1/del/<rid>', methods=['GET'])
def del_result(rid):
    db.LogProc.remove(objectid.ObjectId(rid))
    return redirect('/logprocs/v1/gets/')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
