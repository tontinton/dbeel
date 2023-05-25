from dbeel import DB

assert DB.create("test") == "OK"
assert DB.set("test", "key", "value") == "OK"
assert DB.get("test", "key") == "value"
assert DB.delete("test", "key") == "OK"
assert "key not found" in DB.get("test", "key")
assert DB.drop("test") == "OK"
