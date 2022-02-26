# Practica_BigDataProcessing
//autora: Andrea Araujo Cuquejo

## Speed Layer

**def writeToStorage(dataFrame: DataFrame, storageRootPath: String)**

Se almacenan los datos conseguidos por kafka (formateándolos previamente), en fichero parquet en el directorio /Resources del proyecto.

![image](https://user-images.githubusercontent.com/73897328/155849077-c5a4c7c6-7abd-4ac4-9e7a-c31e49fce549.png)


## Métricas

### Total de bytes recibidos por antena
**def DevicesCountBytesAntenna(dataFrame: DataFrame): DataFrame**

Se agrupan los datos por el id de antena y se hace un sumatorio de bytes,con ventanas de tiempo de 5 min.Se añade la columna type para identificar el tipo  **antenna_bytes_total**.Los datos resultantes son almacenados en la tabla **bytes** de PostgreSQL.

### Total de bytes transmitidos por id de usuario
**def DevicesCountBytesUser(dataFrame: DataFrame): DataFrame**

Se agrupan los datos por el id de usuario y se hace el sumatorio de bytes,con ventanas de tiempo de 5 min. Se añade la columna type para identificar el tipo  **user_bytes_total**. Los datos resultantes son almacenados en la tabla **bytes** de PostgreSQL.


### Total de bytes transmitidos por aplicación
**def DevicesCountBytesApp(dataFrame: DataFrame): DataFrame**

Se agrupan los datos por app y se hace el sumatorio de bytes,con ventanas de tiempo de 5 min. Se añade la columna type para identificar el tipo  **app_bytes_total**. Los datos resultantes son almacenados en la tabla **bytes** de PostgreSQL.


Resultados(tabla BYTES):

![image](https://user-images.githubusercontent.com/73897328/155848350-380046ba-dc34-41a5-8843-20e95ead67a9.png)


## Batch Layer

## Métricas

### Total de bytes recibidos por antena

**def DevicesCountBytesAntenna(dataFrame: DataFrame): DataFrame**

Agrupa datos por id antena y se hace el sumatorio de los bytes, en ventanas de 1 hora.Se añade la columna type para identificar el tipo  **antenna_bytes_total**. Los datos resultantes son almacenados en la tabla **bytes_hourly** de PostgreSQL.

### Total de bytes transmitidos por email de usuario

**def DevicesCountBytesEmail(dataFrame: DataFrame): DataFrame**

Agrupa datos por email y hace el sumatorio de los bytes, en ventanas de 1 hora.Se añade la columna type para identificar el tipo  **email_bytes_total**. Los datos resultantes son almacenados en la tabla **bytes_hourly** de PostgreSQL.

### Total de bytes transmitidos por aplicación

**def DevicesCountBytesApp(dataFrame: DataFrame): DataFrame**

Agrupa datos por app y hace el sumatorio de los bytes, en ventanas de 1 hora.Se añade la columna type para identificar el tipo  **app_bytes_total**. Los datos resultantes son almacenados en la tabla **bytes_hourly** de PostgreSQL.


### Email de usuarios que han sobrepasado la cuota por hora

**def DevicesEmailSuperiorCuota(dataFrame: DataFrame): DataFrame**

Agrupa los datos por email y hace el sumatorio de los bytes, en ventanas de 1 hora, y se filtran por los email que superan la cuota de bytes.


Resultados:

Tabla bytes_hourly

![image](https://user-images.githubusercontent.com/73897328/155853154-79c86ad2-bb5b-46da-ab69-8cf4518a1319.png)



Tabla user_quota_limit

![image](https://user-images.githubusercontent.com/73897328/155852893-d032d81d-eaca-49a3-96e3-bb8934495aab.png)




