const sql = require("mssql");
const fs = require("fs");
const Papa = require("papaparse");
require("dotenv").config();
const checks = require("./checks");

// run the checks for the environment variables
checks();

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

const periodQuery = `PeriodKey IN (${
  periodFilter.length > 0 ? periodFilter.map((period) => `'${period}'`) : "'%'"
})`;

// combine filters into one query
const filterQueryString = `${
  periodFilter.length > 0 ? `AND ${periodQuery}` : ""
}`;

const connect = async () => {
  try {
    // connect to the sql server
    await sql.connect(
      `Server=${process.env.DB_SERVER},${process.env.DB_PORT};Database=${process.env.DB_NAME};User Id=${process.env.DB_USER};Password=${process.env.DB_PASS};Encrypt=false`
    );

    const rows = [];
    const queryPromiseArray = [];

    // if there are no filters, query all rows
    if (accountKeyFilter.length === 0) {
      queryPromiseArray.push(
        sql.query(
          `SELECT * FROM ${process.env.DB_SCHEMA}.${process.env.DB_TABLE} ${filterQueryString}`
        )
      );
    }
    // else query only filtered rows, one by one
    accountKeyFilter.forEach((accountKey) => {
      queryPromiseArray.push(
        sql.query(
          `SELECT * FROM ${process.env.DB_SCHEMA}.${process.env.DB_TABLE} WHERE accountkey = '${accountKey}' ${filterQueryString}`
        )
      );
    });

    // wait for all queries to finish and push the results to the array
    await Promise.all(queryPromiseArray).then((results) => {
      results.forEach((result) => {
        rows.push(...result.recordset);
      });
    });

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
