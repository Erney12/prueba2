import sqlite3

conn= sqlite3.connect("tabla_estudiantes.db")
cursor= conn.cursor()

cursor.execute("""CREATE TABLE estudiantes(
            nombre TEXT,
            edad INTEGER,
            estatura REAL
    )""")

cursor.execute("""INSERT INTO estudiantes VALUES("mark", 27, 1.9)""")

list_estudiantes=[
    ("jhon", 21, 1.8),
    ("david", 35, 1.7),
    ("michael", 19, 1.73)
    ]
cursor.executemany("INSERT INTO estudiantes VALUES(?, ?, ?)", list_estudiantes)

conn.commit()
conn.closes