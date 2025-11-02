import asyncio
import aiosqlite
import random
import os

DB_PATH = "test.db"


async def main():
    # Delete old database file to start fresh (optional)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # Connect to SQLite file-based database
    async with aiosqlite.connect(DB_PATH) as db:
        # Create table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER,
                city TEXT
            )
        """)

        # Example data for names and cities
        names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah"]
        cities = ["Prague", "Kyiv", "Warsaw", "Berlin", "Paris", "Madrid", "Rome", "Vienna"]

        # Async function to insert one row
        async def insert_user(name, age, city):
            await db.execute(
                "INSERT INTO users (name, age, city) VALUES (?, ?, ?)",
                (name, age, city)
            )

        chunk = 200  # Number of tasks to run simultaneously
        tasks = []
        pended = 0

        # Insert 1000 random users
        for x in range(1000):
            name = random.choice(names)
            age = random.randint(15, 70)
            city = random.choice(cities)
            tasks.append(asyncio.create_task(insert_user(name, age, city)))
            pended += 1

            # Once we reach chunk size, run all tasks
            if len(tasks) == chunk or pended == 1000:
                await asyncio.gather(*tasks)
                await db.commit()  # Save changes to disk
                tasks = []
                print(f"{pended} records inserted")

        # Check total number of rows in the table
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total = (await cursor.fetchone())[0]
            print(f"\nTotal rows in table: {total}")

        # Print 5 sample rows
        async with db.execute("SELECT * FROM users LIMIT 5") as cursor:
            async for row in cursor:
                print(row)

# Run the async main function
asyncio.run(main())
