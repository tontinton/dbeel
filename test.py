from dbeel import DB

document = {"hello": "world"}

assert DB.create("test") == "OK"
assert DB.set("test", "key", document) == "OK"
assert DB.get("test", "key") == document
assert DB.delete("test", "key") == "OK"
assert "key not found" in DB.get("test", "key")
assert DB.drop("test") == "OK"
