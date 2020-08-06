const express = require('express')

const app = express()
app.use(express.static('./build/site'))

app.listen(8000, () => console.log('📘 http://localhost:8000'))