# db_converter

# How to run:

```
nohup python36 db_converter.py \
	--packet-name=my_packet \
	--db-name=ALL
    > /dev/null 2>&1 &
```

# Dependencies and installation

Python 3.x with modules: sqlparse, requests

```
yum install -y python38
# if pip not installed
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.8 get-pip.py
pip3.8 install sqlparse
pip3.8 install requests
```
