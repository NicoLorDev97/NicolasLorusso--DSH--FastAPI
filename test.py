import pandas as pd
import numpy as np
import pymysql
import json
from Configuracion import password

conexion = pymysql.connect(host="localhost",
                           user = "root",
                           password=password,
                           database="proyectoindividual"
                           )

cursor = conexion.cursor()

cursor.execute("SELECT d.Full_Name, COUNT(vista.DriverId) as Ganadas, c.Nationality as Nacionalidad, vista.DriverId FROM (SELECT r.DriverId, Position, r.ConstructorId FROM results r WHERE Position = 1 ORDER BY r.DriverId) as vista INNER JOIN drivers d ON vista.DriverId = d.DriverId INNER JOIN constructors c ON c.ConstructorId =  vista.ConstructorId WHERE c.Nationality = 'American' OR c.Nationality = 'British' GROUP BY r.DriverId LIMIT 1;")

