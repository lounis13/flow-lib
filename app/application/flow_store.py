from app.infra.flow import SQLiteStore

store = SQLiteStore("nb.db")
store.open()