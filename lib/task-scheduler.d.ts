import type { Db as MongoDb } from 'mongodb';
type TaskWorker = (taskInstance: TaskInstance) => Promise<string | void> | string | undefined;
export interface TaskInstance {
    signalProgress(): Promise<void>;
}
export declare class TaskScheduler {
    private taskWorkers;
    private pollingIntervalId;
    pollingInterval: number;
    processingTimeout: number;
    timezone: string;
    errorHandler: (...data: any[]) => void;
    collectionName: string;
    databasePromise: (() => Promise<MongoDb>) | undefined;
    scheduleTask(id: string, cronSchedule: string, taskWorker: TaskWorker): Promise<void>;
    stopPolling(): void;
    private findTask;
    private startPolling;
    private poll;
    private processTask;
    private releaseTask;
    private completeTask;
    private receiveTask;
    private updateTask;
    private insertTask;
    private getMongoCollection;
    private getFormattedTime;
}
export {};
//# sourceMappingURL=task-scheduler.d.ts.map