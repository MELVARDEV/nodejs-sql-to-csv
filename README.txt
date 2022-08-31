Running the application:
  - Run "npm install" inside of the source directory
  - Enter server details into ".env" file
  - Start the application with "npm start" or "node index.js"


Query filters:

The application will return filtered results if any filters have been provided. Otherwise, all data will be returned.

Example account_filter.txt:
  account1,account2,account3,account4
Example period_filter.txt:
  20220101,20220102,20220103,20220104

Each filter key is separated by a comma. Keys can be placed in a single line or one per line, as long as each key is followed by a comma.