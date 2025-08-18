const express = require("express")
const app = express();
const PORT = 5000;

app.use(express.json());

// app.get("/ingest", (req, res) => {
//   console.log("entered ingest endpoint");
//   res.json({ status: "ok" });
// });

app.post("/ingest", (req, res) => {
  console.log("Received log:", req.body);
  res.json({ status: "ok" });
});

app.listen(PORT, () => {
  console.log(`Log server running at http://localhost:${PORT}`);
});
