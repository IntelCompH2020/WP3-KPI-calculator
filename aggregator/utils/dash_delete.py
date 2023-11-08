import psycopg2


def delete_table_by_id(table_name, id):
    # Database connection details
    host = "192.168.1.240"
    port = 5432
    database = "sti-viewer"
    user = "sti-viewer"
    password = "H2s9r82BG25HkadKb7MDwBCD"

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )

    # Create a cursor to interact with the database
    cur = conn.cursor()

    # Execute the DELETE statement to delete the table by ID
    query = f"DELETE FROM {table_name} WHERE key = %s"
    cur.execute(query, (id,))

    # Commit the transaction to make the deletion permanent
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()


# Call the function to delete a table by ID
table_name = "user_settings"
id = "ipourgio"
delete_table_by_id(table_name, id)
