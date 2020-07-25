import sys
sys.path.append("../../")
import unittest

#from NORM import SQLiteHandler
class TestInsert(unittest.TestCase):
    def setUp(self):
        from NORM import SQLiteHandler as db

        self.main = db.database("self.main.db",
        tables = [
            db.table("Cars",
                columns = [
                    db.column("Manufacturer",str),
                    db.column("Model",str),
                    db.column("Year",int)
                    ]
                )
            ]
        )


    def testinsert(self):


        self.main["Cars"].append({"Manufacturer":"Ferrari","Model":"Enzo","Year":1999})
        self.main["Cars"].append([{"Manufacturer":"Ferrari","Model":"Enzo","Year":1999},{"Manufacturer":"Audi","Model":"R8","Year":2007}])



class TestFetch(unittest.TestCase):
    def setUp(self):
        from NORM import SQLiteHandler as db
        self.main = db.database("self.main.db",
        tables = [
            db.table("Cars",
                columns = [
                    db.column("Manufacturer",str),
                    db.column("Model",str),
                    db.column("Year",int)
                    ]
                )
            ]
        )


    def test_fetch(self):

        self.assertEqual(self.main["Cars"][self.main["Cars"]["Manufacturer"] == 'Ferrari'],[])
class TestModify(unittest.TestCase):
    def setUp(self):
        from NORM import SQLiteHandler as db
        self.main = db.database("self.main.db",
        tables = [
            db.table("Cars",
                columns = [
                    db.column("Manufacturer",str),
                    db.column("Model",str),
                    db.column("Year",int)
                    ]
                )
            ]
        )


    def test_modify_increment(self):


        self.main["Cars"]["Year"]+=10
        #This increments all the year by 10
    def test_modify_set(self):
        self.main["Cars"][self.main["Cars"]["Manufacturer"]=="Volkswagen","Manufacturer"] = "Vw"
        #This searches for all the Cars with Manufacturers Volkswagen and then store in the Manufacturers column "vw" which satisfies the query.

class TestDelete(unittest.TestCase):
    def setUp(self):

        from NORM import SQLiteHandler as db
        self.main = db.database("self.main.db",
        tables = [
            db.table("Cars",
                columns = [
                    db.column("Manufacturer",str),
                    db.column("Model",str),
                    db.column("Year",int)
                    ]
                )
            ]
        )

    def test_delete_attribute(self):

        del self.main["Cars"][self.main["Cars"]["Manufacturer"]=="Ferrari","Year"]
        #Removes the Year attribute of all the Cars where Manufacturer is Ferrari
    def test_delete_row(self):

        del self.main["Cars"][self.main["Cars"]["Manufacturer"]=="Ferrari"]
        #Deletes all the rows where Manufacturer is Ferrari
    def test_delete_column(self):

        del self.main["Cars"]["Year"]
        #This feature is not working due to sqlite3 constraints#Deletes the column year
    def test_delete_table(self):

        del self.main["Cars"]
    def tearDown(self):
        self.main.close()


if __name__ == "__self.main__":
    unittest.self.main()
