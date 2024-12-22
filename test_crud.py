# test_crud.py

from crud import CRUD

# Connection details
USER = 'aacuser'
PASSWORD = 'jackelope'  
HOST = 'nv-desktop-services.apporto.com'
PORT = 31580
DB_NAME = 'AAC'
COLLECTION_NAME = 'animals'

# Instantiate the CRUD object
crud = CRUD(user=USER, password=PASSWORD, host=HOST, port=PORT, db_name=DB_NAME, collection_name=COLLECTION_NAME)

# 1. Create: Insert a new document
new_animal = {
    "Name": "Sam",
    "Breed": "Dalmation",
    "Age": 3,
    "Color": "White",
    "Outcome": "Adopted",
    "Date": "2025-12-08"
}

print("\n--- Create Operation ---")
insert_success = crud.create(new_animal)
print(f"Insert Successful: {insert_success}")

# 2. Read: Retrieve the inserted document
print("\n--- Read Operation ---")
query = {"Name": "Sam"}
results = crud.read(query)
print("Retrieved Documents:")
for doc in results:
    print(doc)

# 3. Update: Update the Age of the document
print("\n--- Update Operation ---")
update_query = {"Name": "Sam"}
new_values = {"Age": 5}
modified_count = crud.update(update_query, new_values)
print(f"Number of documents updated: {modified_count}")

# Verify the update
print("\n--- Read After Update ---")
results = crud.read(query)
print("Retrieved Documents After Update:")
for doc in results:
    print(doc)

# 4. Delete: Delete the inserted document
print("\n--- Delete Operation ---")
delete_query = {"Name": "Sam"}
deleted_count = crud.delete(delete_query)
print(f"Number of documents deleted: {deleted_count}")

# Verify the deletion
print("\n--- Read After Deletion ---")
results = crud.read(delete_query)
print("Retrieved Documents After Deletion:")
for doc in results:
    print(doc)
