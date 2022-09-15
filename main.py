from fastapi import *
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse
import pymysql
from Configuracion import password
import json

conexion = pymysql.connect(host="localhost",
                           user = "root",
                           password=password,
                           database="proyectoindividual"
                           )

cursor = conexion.cursor()


app = FastAPI(title="Proyecto_Individual",
              description="API del Proyecto Individual Henry"
)

@app.get("/CantidadCarreras")
async def Respuesta1():
    cursor.execute("SELECT  anio, COUNT(anio) as Carreras FROM races GROUP BY anio ORDER BY Carreras DESC LIMIT 1;")

    for dato in cursor:
        valor = dato[0]
    
    return JSONResponse(content = f"El año donde mas carreras se corrieron fue {valor}.")


@app.get("/PilotoGanador")
async def Respuesta2():
    cursor.execute("SELECT d.Full_Name, COUNT(vista.DriverId) as Ganadas, vista.DriverId FROM (SELECT r.DriverId, Position FROM results r WHERE Position = 1 ORDER BY r.DriverId) as vista INNER JOIN drivers d WHERE vista.DriverId = d.DriverId GROUP BY r.DriverId LIMIT 1;")

    for dato in cursor:
        nombre = dato[0]
        carreras = dato[1]
    
    return JSONResponse(content = f"El piloto mas ganador fue {nombre} con {carreras} carreras ganadas.") 

@app.get("/CircuitoMasCorrido")      
async def Respuesta3():
    cursor.execute("SELECT COUNT(r.RaceId) as CarreraId, r.CircuitId, c.Nombre, c.Country FROM races r INNER JOIN circuits c WHERE r.CircuitId = c.CircuitId GROUP BY CircuitId ORDER BY CarreraId DESC LIMIT 1;")
    for dato in cursor:
        cantidad = dato[0]
        nombre = dato[2]
        pais = dato[3]
    return JSONResponse(content= f"El circuito mas recorrido fue el '{nombre}' el cual se corrio {cantidad} veces y queda en {pais} ")
        

@app.get("/PilotoConstructor")
async def Respuesta4():
    cursor.execute("SELECT d.Full_Name, COUNT(vista.DriverId) as Ganadas, c.Nationality as Nacionalidad, vista.DriverId FROM (SELECT r.DriverId, Position, r.ConstructorId FROM results r WHERE Position = 1 ORDER BY r.DriverId) as vista INNER JOIN drivers d ON vista.DriverId = d.DriverId INNER JOIN constructors c ON c.ConstructorId =  vista.ConstructorId WHERE c.Nationality = 'American' OR c.Nationality = 'British' GROUP BY r.DriverId LIMIT 1;")
    for dato in cursor:
        nombre = dato[0]
        cantidad = dato[1]
        nacionalidad = dato[2]
    return JSONResponse(content = f"El piloto {nombre} es el mayor ganador cuyo constructor sea 'British' o 'American' con {cantidad} carreras ganadas y en este caso, el constructor es {nacionalidad}")