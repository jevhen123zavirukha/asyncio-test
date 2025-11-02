import asyncio
import aiosqlite
import random
import os

DB_PATH = "test.db"


async def main():
    # Delete old database file if it exists — start fresh each time
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    async with aiosqlite.connect(DB_PATH) as db:
        # Create the table (if it doesn’t exist)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER,
                city TEXT
            )
        """)

        # Example data
        names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah"]
        cities = ["Prague", "Kyiv", "Warsaw", "Berlin", "Paris", "Madrid", "Rome", "Vienna"]

        # Async function to insert one row
        async def make_request(name, age, city):
            await db.execute(
                "INSERT INTO users (name, age, city) VALUES (?, ?, ?)",
                (name, age, city)
            )

        chunk = 200  # number of tasks to execute at the same time
        tasks = []
        pended = 0

        # Create and insert 1000 records
        for x in range(1000):
            name = random.choice(names)
            city = random.choice(cities)
            age = random.randint(15, 70)
            tasks.append(asyncio.create_task(make_request(name, age, city)))
            pended += 1

            # 200 tasks -> run them all together
            if len(tasks) == chunk or pended == 1000:
                await asyncio.gather(*tasks)  # run 200 inserts in parallel
                await db.commit()              # save changes to the DB
                tasks = []                     # clear task list
                print(f"{pended} records inserted")

        # Check how many rows are in the table
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            count = (await cursor.fetchone())[0]
            print(f"\ntotal rows in table: {count}")

        # Display first 5 rows as a preview
        async with db.execute("SELECT * FROM users LIMIT 5") as cursor:
            async for row in cursor:
                print(row)

# Run
asyncio.run(main())
