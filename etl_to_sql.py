import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine


##################LEYENDO DATASETS

dfDrivers = pd.read_json("./Datasets/drivers.json", lines =True)
dfConstructors = pd.read_json("./Datasets/constructors.json",lines=True)
dfCircuits = pd.read_csv("./Datasets/circuits.csv")
dfRaces = pd.read_csv("./Datasets/races.csv")
dfResults = pd.read_json("./Datasets/results.json",lines=True)

### Convertir a JSON para la api


#######################RENOMBRANDO COLUMNAS
dfDrivers = dfDrivers.rename(columns={"driverId":"DriverId", "driverRef" : "DriverRef", "number" : "Number", "code":"Code",
                                      "name" : "Full_Name", "dob":"DoB", "nationality":"Nationality","url":"Url"})

dfConstructors = dfConstructors.rename(columns={"constructorId" : "ConstructorId", "constructorRef" : "ConstructorRef",
                                                "name" : "Full_Name", "nationality" : "Nationality", "url" : "Url" })

dfResults = dfResults.rename(columns={"resultId": "ResultId", "raceId" : "RaceId", "driverId" : "DriverId", "constructorId":"ConstructorId","number":"number",
                                      "grid":"Grid","position":"Position","points":"Points", "laps":"Laps","time":"Time","milliseconds":"Milliseconds",
                                      "fastestLap":"FastestLap","rank":"Rank","fastestLapTime":"FastestLapTime", "fastestLapSpeed":"FastestLapSpeed","statusId":"StatusId"})

dfCircuits = dfCircuits.rename(columns={"circuitId": "CircuitId", "circuitRef" : "CircuitRef", "name" : "Nombre", "location":"Location","country":"Country",
                                        "lat":"Latitud","lng":"Longitud","alt":"Alt", "url":"Url"})

dfRaces = dfRaces.rename(columns={"raceId": "RaceId", "year" : "Anio", "round" : "Round", "circuitId":"CircuitId","name":"Name",
                                "date":"Date","time":"Time","alt":"Alt", "url":"Url"})
######################CAMBIANDO VALORES

#Drivers
dfDrivers = dfDrivers.replace({"\\N": 0})

rowsDrivers = np.shape(dfDrivers)

for i in range(rowsDrivers[0]):
    dato = dfDrivers["Full_Name"].iloc[i]
    dfDrivers["Full_Name"].iloc[i] = dato["forename"] + " " + dato["surname"]
    dfDrivers["DriverRef"].iloc[i] = dfDrivers["DriverRef"].iloc[i].capitalize()


# Constructors
rowsConstructors = np.shape(dfConstructors)
dfConstructors = dfConstructors.drop(columns=["ConstructorRef"])

for i in range(rowsConstructors[0]):
    if "-" in dfConstructors["Full_Name"].iloc[i]:
        dfConstructors["Full_Name"].iloc[i] = dfConstructors["Full_Name"].iloc[i].replace("-"," ")
    
# Results

dfResults = dfResults.drop(columns=["positionText", "positionOrder"])


dfResults = dfResults.replace({"\\N": 0})

# Circuits

rowsCircuits = np.shape(dfCircuits)

for i in range(rowsCircuits[0]):
    if "_" in  dfCircuits["CircuitRef"].iloc[i]:
         dfCircuits["CircuitRef"].iloc[i] =  dfCircuits["CircuitRef"].iloc[i].replace("_"," ")
    dfCircuits["CircuitRef"].iloc[i] = dfCircuits["CircuitRef"].iloc[i].title()
        

# Races

dfRaces = dfRaces.replace({"\\N": 0})


cadena_conexion = "mysql+pymysql://root@localhost:3306/proyectoindividual"
conexion = create_engine(cadena_conexion)

### TO MYSQL


dfDrivers.to_sql(name="drivers", con=conexion,index=False)
dfConstructors.to_sql(name="constructors",con=conexion,index=False)
dfResults.to_sql(name="results", con=conexion, index=False)
dfCircuits.to_sql(name="circuits", con=conexion, index=False)
dfRaces.to_sql(name="races",con=conexion,index=False)