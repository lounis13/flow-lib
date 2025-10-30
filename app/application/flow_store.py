from app.infra.flow import SQLStore

store = SQLStore("sqlite:///nb.db")
store.open()