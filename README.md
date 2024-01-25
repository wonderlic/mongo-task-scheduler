# mongo-task-scheduler

[![NPM version](https://badge.fury.io/js/mongo-task-scheduler.svg)](http://badge.fury.io/js/mongo-task-scheduler)

### A promise based task scheduler for Node.js using a mongodb backing store.

## Package Dependency Notice

NOTE: This package requires MongoDB server 5+ and MongoDB nodejs driver 4+

## Usage

### Installation

```
npm install mongo-task-scheduler --save
```

### Require & Instantiate

Example:

```javascript
const TaskScheduler = require('mongo-task-scheduler');

const scheduler = new TaskScheduler();
```

### Database Configuration

Set the .databasePromise property to a function that returns a promise that (eventually) returns a mongodb database connection.

Example:

```javascript
const MongoClient = require('mongodb').MongoClient;

const mongoUri = ...;
const mongoOptions = ...;

scheduler.databasePromise = function() {
  // Return a promise to return a mongo database connection here...
  return MongoClient.connect(mongoUri, mongoOptions);
};
```

### Schedule one or more Tasks

Use the .scheduleTask method to schedule a task and provide a processing method.

`scheduler.scheduleTask(id, cronSchedule, worker)`

Scheduling a task will cause the task scheduler to immediately start polling for work for that specified task. By default, the task scheduler looks for tasks that need running at once every second (configurable by overridding the .pollingInterval property). Polling will continue to occur until the .stopPolling() method is called.

The second parameter `cronSchedule` specifies the how often to run the task and uses cron syntax:

```
*    *    *    *    *    *
┬    ┬    ┬    ┬    ┬    ┬
│    │    │    │    │    |
│    │    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
│    │    │    │    └───── month (1 - 12)
│    │    │    └────────── day of month (1 - 31)
│    │    └─────────────── hour (0 - 23)
│    └──────────────────── minute (0 - 59)
└───────────────────────── second (0 - 59, optional)
```

The third parameter `worker` specifies the processing method for the task. When the task needs to be run, the provided processing method will be called.

Example (runs every morning at 5 AM):

```javascript
scheduler.scheduleTask('daily-reminder', '0 5 * * *', function () {
  // ... send the reminder here
  // this can return a promise (chain) if needed.
});
```

## License

(The MIT License)

Copyright (c) 2014-2023 Wonderlic, Inc. <SoftwareDevelopment@wonderlic.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
