import json
import boto3
import csv
import io
import pymysql
from datetime import datetime

# Configura las credenciales y detalles de la base de datos MySQL RDS
db_config = {
    "host": "challenge-1.chqyrfmnsyyz.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "challengeaws",
    "db": "challenge",
}

s3Cliente = boto3.client('s3')

# Tamaño del lote para inserción
BATCH_SIZE = 1000

# Read CSV file content from S3 bucket
def read_data_from_s3(event):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    file_name = s3_file_name.split(".")[0]
    resp = s3Cliente.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    rows = resp['Body'].read().decode('utf-8').splitlines()
    if rows[2] == 'Content-Type: text/csv':
        rows = rows[4:-1]
    csv_reader = csv.reader(rows)
    print("value",rows)
    print(resp)
    return file_name,csv_reader

# Función para insertar carreras en la base de datos
def insert_career_data(data_to_insert,connection):

    # Crear un cursor para ejecutar las consultas
    cursor = connection.cursor()

    # Insertar carreras en la base de datos
    # Utilizar un enunciado preparado para mejorar el rendimiento
    insert_query = "INSERT INTO career (career) VALUES (%s)"
    cursor.executemany(insert_query, data_to_insert)

    # Hacer commit para guardar los cambios
    connection.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()
# Función para insertar cursos en la base de datos
def insert_course_data(data_to_insert,connection):

    # Crear un cursor para ejecutar las consultas
    cursor = connection.cursor()

    # Insertar cursos en la base de datos
    # Utilizar un enunciado preparado para mejorar el rendimiento
    insert_query = "INSERT INTO course (course) VALUES (%s)"
    cursor.executemany(insert_query, data_to_insert)

    # Hacer commit para guardar los cambios
    connection.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()   
    
def insert_student_data(data_to_insert,connection):
    # Crear un cursor para ejecutar las consultas
    cursor = connection.cursor()

    # Insertar estudiantes en la base de datos
    # Utilizar un enunciado preparado para mejorar el rendimiento
    insert_query = "INSERT INTO student (name, enrolment, career_id, course_id) VALUES (%s, %s, %s, %s)"
    total_rows = len(data_to_insert)
    for i in range(0, total_rows, BATCH_SIZE):
        batch = data_to_insert[i:i + BATCH_SIZE]
        cursor.executemany(insert_query, batch)

    #cursor.executemany(insert_query, student_data)
    
    
    # Hacer commit para guardar los cambios
    connection.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()
    
    
def lambda_handler(event, context):
    # TODO implement

    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["db"]
    )
    
    #try:
        # Descarga el archivo CSV desde S3 y lo procesa
    file_name, data = read_data_from_s3(event)
    print(file_name)
    # Prepara los datos para la inserción por lotes
    data_to_insert = []
    invalid_records = []
    for row in data:
        #print(row)
        if file_name == "career":
            if row[1] and row[1].strip():
                data_to_insert.append((row[1],))
            else:
                invalid_records.append(row)
        elif file_name == "course":
            if row[1] and row[1].strip():
                data_to_insert.append((row[1],))
            else:
                invalid_records.append(row)
        elif file_name == "students":
            if len(row) == 5:  # Asumiendo que hay 4 campos requeridos en cada registro
                try:
                    value1 = row[1]
                    value2 = row[2]
                    value3 = int(row[3])
                    value4 = int(row[4])
                    
                    # Verificar si la fecha de inscripción está en formato ISO
                    try:
                        datetime.strptime(value2, "%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        # Capturar registros inválidos (fecha de inscripción no está en formato ISO)
                        invalid_records.append(row)
                        continue

                    # Realizar otras validaciones si es necesario
                    
                    data_to_insert.append((value1, value2, value3, value4))
                except ValueError as e:
                    # Capturar registros inválidos (por ejemplo, si el campo 3 o 4 no es un entero válido)
                    invalid_records.append(row)
            else:
                # Capturar registros inválidos (por ejemplo, si el número de campos no coincide)
                invalid_records.append(row)

            #data_to_insert.append((row[1], row[2], int(row[3]), int(row[4])))
        
    if file_name == "career":
        insert_career_data(data_to_insert,connection)

    elif  file_name == "course":
        insert_course_data(data_to_insert,connection)
    elif  file_name == "students":
        insert_student_data(data_to_insert,connection)
        
    print(invalid_records)
    return {
        'statusCode': 200,
        'body': json.dumps('Archivo CSV procesado y datos insertados en la base de datos.')
    }

