from flask import request
from flask_basicauth import BasicAuth
from subprocess import PIPE, STDOUT, run, Popen
import html
import os
import re
import shutil
import logging

import boto3
from boto3.session import Session
from botocore.client import Config
from botocore.handlers import set_list_objects_encoding_type_url

from flask import Flask
app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = 'zenith'
app.config['BASIC_AUTH_PASSWORD'] = os.getenv('BASIC_AUTH_PASSWORD')
app.config['BASIC_AUTH_FORCE'] = True

basic_auth = BasicAuth(app)

minwalpos = 0
maxwalpos = 0

# S3 configuration:

ENDPOINT = os.getenv('S3_ENDPOINT', 'https://localhost:9000')
ACCESS_KEY = os.getenv('S3_ACCESSKEY', 'minioadmin')
SECRET = os.getenv('S3_SECRET', '')
BUCKET = os.getenv('S3_BUCKET', 'foobucket')

#boto3.set_stream_logger('botocore', logging.DEBUG)

session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET,
                  region_name=os.getenv('S3_REGION', 'auto'))

# needd for google cloud?
session.events.unregister('before-parameter-build.s3.ListObjects',
                          set_list_objects_encoding_type_url)

s3resource = session.resource('s3',
                              endpoint_url=ENDPOINT,
                              verify=False,
                              config=Config(signature_version='s3v4'))
s3bucket = s3resource.Bucket(BUCKET)

s3_client = boto3.client('s3',
                         endpoint_url=ENDPOINT,
                         verify=False,
                         config=Config(signature_version='s3v4'),
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET)


@app.route('/')
def landing():
    return mainpage(None)

def mainpage(operation):
    responsestr = '''
<html>
<body>

'''
    if operation:
        responsestr += operation
        responsestr += '<hr>\n'

    responsestr += '''
<h1>server status:</h1>

''' + server_status() + '''

<h1>storage bucket status:</h1>

''' + wal_summary() + '''

<h1>Actions:</h1>

<form action="/reset_all" method=post id="reset_all"></form>
<button type="submit" form="reset_all">RESET DEMO</button>

<form action="/init_primary" method=post id="init_primary"></form>
<button type="submit" form="init_primary">Init primary</button>

<form action="/zenith_push" method=post id="zenith_push"></form>
<button type="submit" form="zenith_push">Push base image</button>

<form action="/slicedice" method=post id="slicedice"></form>
<button type="submit" form="slicedice">Slice & Dice WAL</button>

<br>
<br>


<form action="/init_standby" method=post id="init_standby">
  <button type="submit" form="init_standby"
    '''
    if minwalpos == 0 or maxwalpos == 0:
        responsestr += 'disabled'
    responsestr += '''

  >Create new standby</button>
  <label for="walpos">at WAL position:</label><br>
  <input type="walpos" id="walpos" name="walpos" pattern="[0-9A-Z]+/[0-9A-Z]+"
    value="''' + walpos_str(maxwalpos) + '''"
    '''
    if minwalpos == 0 or maxwalpos == 0:
        responsestr += 'disabled'
    responsestr += '''
  ><br>
  <input type="range" id="walpos_slider" min="0" max="10" steps="1" value="10"
    oninput="myFunction()"
    '''
    if minwalpos == 0 or maxwalpos == 0:
        responsestr += 'disabled'
    responsestr += '''
  >

<script>
function myFunction() {
  var x = document.getElementById("walpos_slider").value;

  var walpositions = [
'''

    for x in range(0, 10):
        responsestr += '"' + walpos_str(int(minwalpos + (maxwalpos - minwalpos) * (x/10))) + '", '
    responsestr += '"' + walpos_str(maxwalpos) + '"'

    responsestr += '''
  ];

  document.getElementById("walpos").value = walpositions[x];
}
</script>

</form>

</body>
</html>
'''
    return responsestr

def server_status():
    dirs = os.listdir("pgdatadirs")
    dirs.sort()

    resultstr = ''
    if dirs:
        for dirname in dirs:

            result = run("pg_ctl status -D pgdatadirs/" + dirname, stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)
            resultstr = resultstr + dirname + ": " + result.stdout + "<br>\n<br>\n"
    else:
        resultstr += "no data directories<br>\n"

    return resultstr

@app.route('/list_bucket')
def list_bucket():

    response = 'cloud bucket contents:'

    for file in s3bucket.objects.all():
        response = response + html.escape(file.key) + '<br>\n'

    return mainpage(response)

def walpos_str(walpos):
    return '{:X}/{:X}'.format(walpos >> 32, walpos & 0xFFFFFFFF)

def wal_summary():

    nonrelimages = []
    nonrelwal = []
    walsegments = []

    minwal = 0
    maxwal = 0
    maxseqwal = 0

    for file in s3bucket.objects.all():
        path = file.key
        match = re.search('nonreldata/nonrel_([0-9A-F]+).tar', path)
        if match:
            nonrelimages.append(path)

            wal = int(match.group(1), 16)
            if minwal == 0 or wal < minwal:
                minwal = wal

        match = re.search('nonreldata/nonrel_([0-9A-F]+)-([0-9A-F]+)', path)
        if match:
            nonrelwal.append(path)

            endwal = int(match.group(2), 16)
            if endwal > maxwal:
                maxwal = endwal

        match = re.search('walarchive/([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})', path)
        if match:
            walsegments.append(path)

            tli = int(match.group(1), 16)
            logno = int(match.group(2), 16)
            segno = int(match.group(3), 16)
            # FIXME: this assumes default 16 MB wal segment size
            logsegno = logno * (0x100000000 / (16*1024*1024)) + segno

            seqwal = (logsegno + 1) * (16*1024*1024)

            if seqwal > maxseqwal:
                maxseqwal = seqwal;

    responsestr = ''
    if minwal:
        responsestr += 'base image at ' + walpos_str(minwal) + '<br>\n'
        responsestr += '<br><br>base images:<br>'

        for x in nonrelimages:
            responsestr += x + "<br>\n"
    else:
        responsestr += "no base image<br>\n"

    if maxwal:
        responsestr += 'Sliced WAL is available up to '+ walpos_str(maxwal) + '<br>\n'
        responsestr += 'sliced nonrel WAL:<br>'
        for x in nonrelwal:
            responsestr += x + "<br>\n"
    else:
        responsestr += 'no sliced WAL available<br>\n'

    if walsegments:
        responsestr += "raw WAL archive: <br>\n" + walsegments[0]
        responsestr += " - " + walsegments[-1] + "<br>\n"
        #for x in walsegments:
        #    responsestr += x + "<br>\n"
    else:
        responsestr += "no archived raw WAL"

    global minwalpos
    minwalpos = minwal
    global maxwalpos
    maxwalpos = maxwal

    return responsestr


def print_cmd_result(cmd_result):
    return print_cmd_result_ex(cmd_result.args, cmd_result.returncode, cmd_result.stdout)

def print_cmd_result_ex(cmd, returncode, stdout):
    return '''
ran command:<br>
<blockquote>
''' + html.escape(str(cmd)).replace("\n","<br/>\n") + '''
</blockquote>

It returned code
''' + str(returncode) + '''
<br>
stdout/stderr:
<blockquote>
''' + html.escape(stdout).replace("\n","<br/>\n") + '''
</blockquote>
<br>
'''


@app.route('/init_primary', methods=['GET', 'POST'])
def init_primary():
    initdb_result = run("initdb -D pgdatadirs/primary --username=zenith --pwfile=pg-password.txt", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)
    if initdb_result.returncode != 0:
        return mainpage(print_cmd_result(initdb_result))
    
    # Append archive_mode and archive_command and port to postgresql.conf
    f=open("pgdatadirs/primary/postgresql.conf", "a+")
    f.write("listen_addresses='*'\n")
    f.write("archive_mode=on\n")
    f.write("archive_command='zenith_push --archive-wal-path=%p --archive-wal-fname=%f dummyurl'\n")
    f.write("ssl=on\n")
    f.close()

    f=open("pgdatadirs/primary/pg_hba.conf", "a+")
    f.write("# allow SSL connections with password from anywhere\n")
    f.write("hostssl    all             all             0.0.0.0/0           md5\n")
    f.write("hostssl    all             all             ::0/0               md5\n")
    f.close()

    shutil.copyfile("server.crt", "pgdatadirs/primary/server.crt")
    shutil.copyfile("server.key", "pgdatadirs/primary/server.key")
    os.chmod("pgdatadirs/primary/server.key", 0o0600)
    
    start_proc = Popen(args=["pg_ctl", "start", "-D", "pgdatadirs/primary", "-l", "pgdatadirs/primary/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    start_rc = start_proc.wait()
    start_stdout, start_stderr = start_proc.communicate()

    responsestr = print_cmd_result(initdb_result) + '\n'
    responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout)

    return mainpage(responsestr)

@app.route('/zenith_push', methods=['GET', 'POST'])
def zenith_push():
    # Stop the primary if it's running
    stop_result = run(args=["pg_ctl", "stop", "-D", "pgdatadirs/primary"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    
    # Call zenith_push
    push_result = run("zenith_push -D pgdatadirs/primary dummyurl", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)

    # Restart the primary
    start_proc = Popen(args=["pg_ctl", "start", "-D", "pgdatadirs/primary", "-l", "pgdatadirs/primary/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    start_rc = start_proc.wait()
    start_stdout, start_stderr = start_proc.communicate()
    
    responsestr = print_cmd_result(stop_result) + '\n'
    responsestr += print_cmd_result(push_result) + '\n'
    responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout) + '\n'

    return mainpage(responsestr)

@app.route('/init_standby', methods=['GET', 'POST'])
def init_standby():

    walpos = request.form.get('walpos')
    if not walpos:
        return 'no walpos'
    
    dirs = os.listdir("pgdatadirs")

    last_port = 5432

    for dirname in dirs:

        standby_match = re.search('standby_([0-9]+)', dirname)
        if standby_match:
            port = int(standby_match.group(1))
            if port > last_port:
                last_port = port

    standby_port = last_port + 1

    standby_dir = "pgdatadirs/standby_" + str(standby_port)

    # Call zenith_restore
    restore_result = run(["zenith_restore", "--end=" + walpos, "-D", standby_dir], stdout=PIPE, stderr=STDOUT, encoding='latin1')
    responsestr = print_cmd_result(restore_result)

    if restore_result.returncode == 0:
        # Append hot_standby and port to postgresql.conf
        f=open(standby_dir + "/postgresql.conf", "a+")
        f.write("hot_standby=on\n")
        f.write("port=" + str(standby_port) + "\n")
        f.close()

        start_proc = Popen(args=["pg_ctl", "start", "-D", standby_dir, "-l", standby_dir + "/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
        start_rc = start_proc.wait()
        start_stdout, start_stderr = start_proc.communicate()
        responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout)

    return mainpage(responsestr)

@app.route('/slicedice', methods=['GET', 'POST'])
def run_slicedice():
    result = run("zenith_slicedice dummyurl", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)
    
    responsestr = print_cmd_result(result)

    return mainpage(responsestr)

@app.route('/reset_all', methods=['POST'])
def reset_all():
    result = run("pkill -9 postgres", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)

    dirs = os.listdir("pgdatadirs")
    for dirname in dirs:
        shutil.rmtree('pgdatadirs/' + dirname)
        
    for file in s3bucket.objects.all():
        s3_client.delete_object(Bucket = BUCKET, Key = file.key)

    responsestr = print_cmd_result(result) + '''
Deleted all Postgres datadirs<br>
Deleted all files in object storage bucket<br>
'''
    return mainpage(responsestr)


if __name__ == '__main__':
    app.run()
