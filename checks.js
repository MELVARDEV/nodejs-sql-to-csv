require("dotenv").config();
const fs = require("fs");

module.exports = () => {
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
};
