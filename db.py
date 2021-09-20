import asyncio
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time


def create_connection():
    cloud_config = {
        "secure_connect_bundle": "/home/sakshi/python_project/secure-connect-test.zip"
    }
    # auth_provider = PlainTextAuthProvider(
    #     "usbOWtaIqzwUjjRdpFZaAgXZ",
    #     "EbnAmPnknXZyBsvA,FyeyA8fwYiZ+AQZcYRynPO8yJzU1D16eYpLOZJ_f98n2aelkEbC9+cLcth6W5Zla5Dl1B1r5BeO5cZQqLW8Zs49R9-RrvKDPtITMZrdBU4UL2NS",
    # )
    # cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    cluster = Cluster(["127.0.0.1"], 9042)
    return cluster.connect(keyspace="sakshi_keyspace")


def create_table(session):
    # session.execute("drop table test_key.users ")
    session.execute(
        "CREATE TABLE users ( email text PRIMARY KEY, lastname text, age int, firstname text, city text );"
    )


def dosomething(row):
    print(row, "inside func user created successfully")


def set_user(session, lastname, age, city, email, firstname):
    result = session.execute_async(
        "INSERT INTO users (lastname, age, city, email, firstname) VALUES (%s,%s,%s,%s,%s)",
        [lastname, age, city, email, firstname],
    )

    def handle_success(row):
        print("user created")
        dosomething(row)
        return row

    def handle_error(exception):
        print("Failed to fetch user info: %s", exception)

    result.add_callbacks(handle_success, handle_error)


def get_user(session, email):
    result = session.execute_async("SELECT * FROM users WHERE email = %s", [email])

    return (
        result.result().one()
        if result.result().one() is not None
        else "User does not exist"
    )


def update_user(session, new_age, email):
    result = session.execute_async(
        "UPDATE users SET age =%s WHERE email = %s", [new_age, email]
    )
    # time.sleep(5)

    print("start")
    print(result)

    def handle_success(row):
        print("row update successsssss!!!!!!!!!!")

        print("suces-------------------------")

    def handle_error(exception):
        print("Failed to fetch user info: %s", exception)

    print("between execute_async nd callback")

    result.add_callbacks(handle_success, handle_error)

    print("end update uer")


def delete_user(session, email):
    print(
        session.execute("DELETE FROM users WHERE email = %s", [email]).all(), "deleted"
    )


async def main():

    session = create_connection()
    lastname = "Jones"
    age = 35
    city = "Austin"
    email = "bob@exampe.com"
    firstname = "Bob"
    new_age = 50

    # create_table(session)

    set_user(session, lastname, age, city, email, firstname)

    get_user(session, email)

    update_user(session, new_age, email)

    get_user(session, email)

    delete_user(session, email)


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(end - start, "seconds taken")
