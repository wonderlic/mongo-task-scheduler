import { afterEach, beforeAll, beforeEach, expect, test, vi } from "vitest";
import { TaskInstance, TaskScheduler } from "./task-scheduler";

import { MongoMemoryServer } from "mongodb-memory-server";

import { MongoClient, Db } from "mongodb";

let mongoServer: MongoMemoryServer;
let db: Db;
let taskScheduler: TaskScheduler;

beforeAll(async () => {
  mongoServer = await MongoMemoryServer.create();
});

beforeEach(async () => {
  vi.useFakeTimers();

  const client = new MongoClient(mongoServer.getUri());

  // Start fresh every test
  await client.db("test").dropDatabase();

  db = client.db("test");

  taskScheduler = new TaskScheduler();
  taskScheduler.databasePromise = () => Promise.resolve(db);
  taskScheduler.errorHandler = vi.fn();
  taskScheduler.timezone = "UTC";
  taskScheduler.pollingInterval = 1000;
  taskScheduler.processingTimeout = 60 * 1000;

  // February 1st, 2025 @ Midnight
  vi.setSystemTime(new Date(Date.UTC(2025, 1, 1, 0, 0, 0)));
});

afterEach(async () => {
  taskScheduler.stopPolling();
  vi.useRealTimers();
});

test("Should pick up the task once the polling interval has passed", async () => {
  const task1 = vi.fn();

  await taskScheduler.scheduleTask("task1", "* 1 * * *", task1);

  expect(task1).not.toHaveBeenCalled();

  // February 1st, 2025 @ 1:01:00
  vi.setSystemTime(new Date(Date.UTC(2025, 1, 1, 1, 1, 0)));

  // Interval hasn't passed so task1 should not be called
  await expect(
    vi.waitUntil(() => {
      return task1.mock.calls.length === 1;
    }),
    "If our time interval hasn't passed, our task should not be called",
  ).rejects.toThrowError();

  await vi.advanceTimersByTimeAsync(1000);

  await vi.waitUntil(() => {
    return task1.mock.calls.length === 1;
  });

  expect(
    task1,
    "After interval has passed, our task should have been called exactly once",
  ).toHaveBeenCalledTimes(1);

  await vi.advanceTimersByTimeAsync(10000);

  await expect(
    vi.waitUntil(() => {
      return task1.mock.calls.length !== 1;
    }),
    "No matter how many intervals we wait, we should not see the task run again until the next time period",
  ).rejects.toThrowError();
});

test("Task times out if it exceeds timeout duration", async () => {

  const done = vi.fn();

  const task1 = vi.fn(async () => {
    await new Promise((resolve) => {
      setTimeout(() => {
        done();
        resolve(0);
      }, 1000 * 61);
    });

    return undefined;
  });

  await taskScheduler.scheduleTask("task1", "* 1 * * *", task1);

  // February 1st, 2025 @ 1:01:00
  vi.setSystemTime(new Date(Date.UTC(2025, 1, 1, 1, 1, 0)));

  await vi.advanceTimersByTimeAsync(1000);

  await vi.waitUntil(() => {
    return task1.mock.calls.length === 1;
  });

  await vi.advanceTimersByTimeAsync(60000);

  expect(task1).toHaveBeenCalledTimes(1);

  await vi.advanceTimersByTimeAsync(20000);

  await vi.waitUntil(() => {
    return done.mock.calls.length === 1;
  });

  expect(task1, "By the time done() is called, task should be reran").toHaveBeenCalledTimes(2);
});

test("Timeout can be delayed by calling signalProgress", async () => {
  const done1 = vi.fn();
  const done2 = vi.fn();

  const task1 = vi.fn(async (taskInstance: TaskInstance) => {
    await new Promise((resolve) => {
      setTimeout(() => {
        taskInstance.signalProgress();
      }, 1000 * 30);

      setTimeout(() => {
        done1();
        resolve(0);
      }, 1000 * 70);
    });

    return undefined;
  });
  const task2 = vi.fn(async (taskInstance: TaskInstance) => {
    await new Promise((resolve) => {
      setTimeout(() => {
        taskInstance.signalProgress();
      }, 1000 * 30);

      setTimeout(() => {
        done2();
        resolve(0);
      }, 1000 * 70);
    });

    return undefined;
  });

  await taskScheduler.scheduleTask("task1", "* 1 * * *", task1);
  await taskScheduler.scheduleTask("task2", "* 1 * * *", task2);

  // February 1st, 2025 @ 1:01:00
  vi.setSystemTime(new Date(Date.UTC(2025, 1, 1, 1, 1, 0)));

  await vi.advanceTimersByTimeAsync(1000);
  await vi.advanceTimersByTimeAsync(1000);

  await vi.waitUntil(() => {
    return task1.mock.calls.length === 1;
  });
  await vi.waitUntil(() => {
    return task2.mock.calls.length === 1;
  });

  await vi.advanceTimersByTimeAsync(60000);

  expect(task1).toHaveBeenCalledTimes(1);
  expect(task2).toHaveBeenCalledTimes(1);

  await vi.advanceTimersByTimeAsync(20000);

  await vi.waitUntil(() => {
    return done1.mock.calls.length === 1;
  });

  await vi.waitUntil(() => {
    return done2.mock.calls.length === 1;
  });

  expect(task1, "We extended the timeout so task1 should still be called once").toHaveBeenCalledTimes(1);
  expect(task2, "We extended the timeout so task2 should still be called once").toHaveBeenCalledTimes(1);
});
