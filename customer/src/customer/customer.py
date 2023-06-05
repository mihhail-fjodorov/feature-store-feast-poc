import random
import psycopg2


customer_uids = [
    "f9152e50-ca83-4e91-83a0-f3a517a65fe4",
    "91aa0850-4069-4968-9e87-5621264fa98f",
    "362b7340-3cad-4366-9348-4a3fd11177d4",
    "10c6d2f1-6083-44f3-8751-a8bdc0599bde",
    "d899be6b-93e1-40ca-8f77-82803bdc7e82",
    "8a06cdef-253f-495f-8892-bcea64da3d2d",
    "4649dfee-b8e6-4735-8463-dd1bbce0bcb5",
    "ad069d8f-3107-440e-84df-e207a982b012",
    "831573fa-cb1b-482c-8c52-432afaa5577e",
    "c4a3bd2a-5cf6-4d01-befd-4edd7da78b7e",
]

transaction_direction = ["IN", "OUT"]

insert_transaction = "INSERT INTO public.transaction (customer_uid, amount, direction, created_at) VALUES (%s, %s, %s, NOW())"


def get_db_connection():
    conn = psycopg2.connect(
        host="feast-customer_db-1",
        port=5432,
        database="postgres",
        user="postgres",
        password="mysecretpassword",
    )
    return conn


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    while True:
        customer_uid = random.choice(customer_uids)
        direction = random.choice(transaction_direction)
        amount = random.randint(0, 500)

        try:
            cur.execute(insert_transaction, (customer_uid, amount, direction))
            conn.commit()
        except Exception as ex:
            print(ex)
            cur.close()
            conn.close()
            raise RuntimeError(ex) from ex


main()
