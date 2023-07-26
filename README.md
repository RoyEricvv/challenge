# challenge: Transfer historical data from CSV files to the new database
we can use AWS Lambda function, S3 and RDS
1. Create a RDS (Mysql)
   use a script in your DB
2. Create a Bucket (S3)
   Upload a file of S3
3. Create a lambda function
   
3.1 Install pymysql using pip on your local machine
```python
python -m venv mi_entorno_virtual
mi_entorno_virtual\Scripts\activate
pip install pymysql
cd mi_entorno_virtual\Lib\site-packages
zip -r pymysql.zip pymysql
```
3.2 Create a layer with the zip created in the before step
3.3 Copy a code of lambda_handler.py
