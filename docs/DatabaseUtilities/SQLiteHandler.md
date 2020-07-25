# SQLiteHandler
V1.0
- Utility for semi-pythonic database.
- Keeps a buffer to highly serialize transactions.
- Systematic logger.

With pythonic syntax, store data in an sql file.

## Known Issues / Features that can be added later
- Vulnerable to SQLInjection
- Storage of Arrays and Dicts can be done by normalisation



## Connecting to a database
``` python
from NORM import SQLiteHandler as db
main = db.database("main.db",tables = [db.table("Cars",columns = [db.column("Manufacturer",str),db.column("Model",str),db.column("Year",int)])])
```
This above snippet asks to connect to <b>main.db</b> with considering only one table that is named "Cars" with columns "Manufacturer","Model" and "Year" with the respective data types.

## Writing Queries  

#### Fetching data
In order to fetch all the models by Ferrari before 1999,
## Connecting to a database
``` python
from NORM import SQLiteHandler as db
main = db.database("main.db",tables = [db.table("Cars",columns = [db.column("Manufacturer",str),db.column("Model",str),db.column("Year",int)])])
print(main["Cars"][main["Cars"]["Manufacturer"] == 'Ferrari'])
#This prints only the cars with Manufacturer as Ferrari
print(main["Cars"][main["Cars"]["Year"]<1999])
#This prints only the cars with Year smaller than 1999
print(main["Cars"][(main["Cars"]["Year"]<1999) and (main["Cars"]["Manufacturer"] == 'Ferrari'])])
#This prints all the information about Ferrari's before 1999
print(main["Cars"][(main["Cars"]["Year"]<1999) and (main["Cars"]["Manufacturer"] == 'Ferrari']),'Model'])
#This only prints the Models of Ferrari's before 1999
```
## Adding Data

```python
from NORM import SQLiteHandler as db
main = db.database("main.db",tables = [db.table("Cars",columns = [db.column("Manufacturer",str),db.column("Model",str),db.column("Year",int)])])
main["Cars"].append({"Manufacturer":"Ferrari","Model":"Enzo","Year":1999})
#Adding single row of data
main["Cars"].append([{"Manufacturer":"Ferrari","Model":"Enzo","Year":1999},{"Manufacturer":"Audi","Model":"R8",Year:2007}])
#Adding multiple rows of data

```

## Modifying Data

``` python
In order to increament the years by 10 and then replace all the Volkswagen with Vw,
from NORM import SQLiteHandler as db
main = db.database("main.db",tables = [db.table("Cars",columns = [db.column("Manufacturer",str),db.column("Model",str),db.column("Year",int)])])
main["Cars"]["Year"]+=10
#This increaments all the year by 10
main["Cars"][main["Cars"]["Manufacturer"]=="Volkswagen","Manufacturer"] = "Vw" # WARNING: Test Failing
#This searches for all the Cars with Manufacturers Volkswagen and then store in the Manufacturers column "vw" which satisfies the query.
```
## Deleting Data
```python
from NORM import SQLiteHandler as db
main = db.database("main.db",tables = [db.table("Cars",columns = [db.column("Manufacturer",str),db.column("Model",str),db.column("Year",int)])])
del main["Cars"][main["Cars"]["Manufacturer"]=="Ferrari","Year"]
#Removes the Year attribute of all the Cars where Manufacturer is Ferrari
del main["Cars"][main["Cars"]["Manufacturer"]=="Ferrari"]
#Deletes all the rows where Manufacturer is Ferrari
del main["Cars"]["Year"]# WARNING: Test Failing
#Deletes the column year
del main["Cars"]
#Deletes the table cars from database
```
