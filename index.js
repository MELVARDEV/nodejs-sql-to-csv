const sql = require("mssql");
const fs = require("fs");
const Papa = require("papaparse");
require("dotenv").config();

// check if all required input is given
if (
  !(
    process.env.DB_USER &&
    process.env.DB_PASS &&
    process.env.DB_SERVER &&
    process.env.DB_NAME &&
    process.env.DB_PORT &&
    process.env.DB_SCHEMA &&
    process.env.DB_TABLE &&
    process.env.MAPPING_FILE &&
    process.env.OUTPUT_FILE &&
    process.env.ACCOUNT_FILTER_FILE &&
    process.env.PERIOD_FILTER_FILE
  )
) {
  throw new Error("Missing environment variables");
}

if (!fs.existsSync(process.env.MAPPING_FILE)) {
  throw new Error("Mapping file does not exist");
}

if (!fs.existsSync(process.env.ACCOUNT_FILTER_FILE)) {
  throw new Error("Account filter file does not exist");
}

if (!fs.existsSync(process.env.PERIOD_FILTER_FILE)) {
  throw new Error("Period filter file does not exist");
}

// load and parse the csv mappings file
const mappingString = fs.readFileSync(process.env.MAPPING_FILE, "utf8");
const mappingRows = Papa.parse(mappingString, {
  header: false,
}).data;

// convert mappingRows to a mapping object
const mappingObject = mappingRows.reduce((acc, row, index) => {
  if (index === 0) {
    return acc;
  }
  acc[row[0]] = row[1];
  return acc;
}, {});

// load and parse the filters from files
const accountKeyFilter = fs.existsSync(process.env.ACCOUNT_FILTER_FILE)
  ? fs
      .readFileSync(process.env.ACCOUNT_FILTER_FILE, "utf8")
      .split(",")
      .map((acc) => acc.trim())
      .filter((a) => a !== "")
  : [];

const periodFilter = fs.existsSync(process.env.PERIOD_FILTER_FILE)
  ? fs
      .readFileSync(process.env.PERIOD_FILTER_FILE, "utf8")
      .split(",")
      .map((acc) => acc.trim())
      .filter((p) => p !== "")
  : [];

// prepare filters for the query
const accountKeyQuery = `WHERE accountKey IN (${
  accountKeyFilter.length > 0
    ? accountKeyFilter.map((acc) => `'${acc}'`)
    : "'%'"
})`;

const periodQuery = `${
  accountKeyFilter.length > 0 ? "AND" : "WHERE"
} PeriodKey IN (${
  periodFilter.length > 0 ? periodFilter.map((period) => `'${period}'`) : "'%'"
})`;

// combine filters into one query
const filterQueryString = `${
  accountKeyFilter.length > 0 ? accountKeyQuery : ""
} ${periodFilter.length > 0 ? periodQuery : ""}`;

const connect = async () => {
  try {
    // connect to the sql server and query the data
    await sql.connect(
      `Server=${process.env.DB_SERVER},${process.env.DB_PORT};Database=${process.env.DB_NAME};User Id=${process.env.DB_USER};Password=${process.env.DB_PASS};Encrypt=false`
    );

    const result = await sql.query(
      `select * from ${process.env.DB_SCHEMA}.${process.env.DB_TABLE} ${filterQueryString}`
    );

    const rows = result.recordset;
    const mappedData = [];

    // map the data according to the the csv format
    rows.forEach((row) => {
      const mappedRow = {};
      Object.keys(mappingObject).forEach((key) => {
        mappedRow[mappingObject[key]] = row[key];
      });
      mappedData.push(mappedRow);
    });

    // convert the data to csv
    const csv = Papa.unparse(mappedData, {
      quotes: true,
    });

    // write the csv to a file
    fs.writeFileSync(process.env.OUTPUT_FILE, csv);
    console.log(
      `${mappedData.length} rows were saved to ${process.env.OUTPUT_FILE}`
    );
  } catch (err) {
    console.log(err);
  }
};

connect();
