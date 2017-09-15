/*
 * Copyright 2017 dmfs GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dmfs.provider.tasks;

import android.content.ContentProviderClient;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.os.Build;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;

import org.dmfs.android.contentpal.OperationsQueue;
import org.dmfs.android.contentpal.RowSnapshot;
import org.dmfs.android.contentpal.Table;
import org.dmfs.android.contentpal.batches.MultiBatch;
import org.dmfs.android.contentpal.batches.SingletonBatch;
import org.dmfs.android.contentpal.operations.Assert;
import org.dmfs.android.contentpal.operations.BulkAssert;
import org.dmfs.android.contentpal.operations.BulkDelete;
import org.dmfs.android.contentpal.operations.Counted;
import org.dmfs.android.contentpal.operations.Delete;
import org.dmfs.android.contentpal.operations.Put;
import org.dmfs.android.contentpal.operations.Related;
import org.dmfs.android.contentpal.predicates.AllOf;
import org.dmfs.android.contentpal.predicates.EqArg;
import org.dmfs.android.contentpal.predicates.ReferringTo;
import org.dmfs.android.contentpal.queues.BasicOperationsQueue;
import org.dmfs.android.contentpal.rowdata.Composite;
import org.dmfs.android.contentpal.rowdata.EmptyRowData;
import org.dmfs.android.contentpal.rowsnapshots.VirtualRowSnapshot;
import org.dmfs.android.contenttestpal.operations.AssertEmptyTable;
import org.dmfs.opentaskspal.tables.InstanceTable;
import org.dmfs.opentaskspal.tables.LocalTaskListsTable;
import org.dmfs.opentaskspal.tables.TaskListScoped;
import org.dmfs.opentaskspal.tables.TaskListsTable;
import org.dmfs.opentaskspal.tables.TasksTable;
import org.dmfs.opentaskspal.tasklists.NameData;
import org.dmfs.opentaskspal.tasks.OriginalInstanceSyncIdData;
import org.dmfs.opentaskspal.tasks.SyncIdData;
import org.dmfs.opentaskspal.tasks.TimeData;
import org.dmfs.opentaskspal.tasks.TitleData;
import org.dmfs.rfc5545.DateTime;
import org.dmfs.rfc5545.Duration;
import org.dmfs.tasks.contract.TaskContract.Instances;
import org.dmfs.tasks.contract.TaskContract.TaskLists;
import org.dmfs.tasks.contract.TaskContract.Tasks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.TimeZone;

import static org.dmfs.android.contenttestpal.ContentMatcher.resultsIn;
import static org.junit.Assert.assertThat;


/**
 * Tests for {@link TaskProvider}.
 *
 * @author Yannic Ahrens
 * @author Gabor Keszthelyi
 */
@RunWith(AndroidJUnit4.class)
public class TaskProviderTest
{
    private ContentResolver mResolver;
    private String mAuthority;
    private Context mContext;
    private ContentProviderClient mClient;


    @Before
    public void setUp() throws Exception
    {
        mContext = InstrumentationRegistry.getTargetContext();
        mResolver = mContext.getContentResolver();
        mAuthority = AuthorityUtil.taskAuthority(mContext);
        mClient = mContext.getContentResolver().acquireContentProviderClient(mAuthority);

        // Assert that tables are empty:
        OperationsQueue queue = new BasicOperationsQueue(mClient);
        queue.enqueue(new MultiBatch(
                new AssertEmptyTable<>(new TasksTable(mAuthority)),
                new AssertEmptyTable<>(new TaskListsTable(mAuthority)),
                new AssertEmptyTable<>(new InstanceTable(mAuthority))));
        queue.flush();
    }


    @After
    public void tearDown() throws Exception
    {
        /*
        TODO When Test Orchestration is available, there will be no need for clear up, every test will run in separate instrumentation
        https://android-developers.googleblog.com/2017/07/android-testing-support-library-10-is.html
        https://developer.android.com/training/testing/junit-runner.html#using-android-test-orchestrator
        */

        // Clear the DB:
        BasicOperationsQueue queue = new BasicOperationsQueue(mClient);
        queue.enqueue(new SingletonBatch(new BulkDelete<>(new LocalTaskListsTable(mAuthority))));
        queue.flush();

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N)
        {
            mClient.close();
        }
        else
        {
            mClient.release();
        }
    }


    @Test
    public void testSingleInsert() throws Exception
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(new TaskListScoped(taskList, new TasksTable(mAuthority)));

        assertThat(new MultiBatch(
                new Put<>(taskList, new NameData("list1")),
                new Put<>(task, new TitleData("task1")),

                // TODO (putting in here to see if they pass in the same transaction as well)
                new Counted<>(1, new BulkAssert<>(new InstanceTable(mAuthority), new ReferringTo<>(Instances.TASK_ID, task))),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID)

        ), resultsIn(mClient,
                new Assert<>(taskList, new NameData("list1")),
                new Assert<>(task, new TitleData("task1")),

                // TODO
                new Counted<>(1, new BulkAssert<>(new InstanceTable(mAuthority), new ReferringTo<>(Instances.TASK_ID, task))),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID)
        ));
    }


    /**
     * Creates three new Tasks. One of them will refer to a different TasksList.
     */
    @Test
    public void testMultipleInserts()
    {
        Table<TaskLists> taskListsTable = new LocalTaskListsTable(mAuthority);
        RowSnapshot<TaskLists> taskList1 = new VirtualRowSnapshot<>(taskListsTable);
        RowSnapshot<TaskLists> taskList2 = new VirtualRowSnapshot<>(taskListsTable);
        RowSnapshot<Tasks> task1 = new VirtualRowSnapshot<>(new TaskListScoped(taskList1, new TasksTable(mAuthority)));
        RowSnapshot<Tasks> task2 = new VirtualRowSnapshot<>(new TaskListScoped(taskList1, new TasksTable(mAuthority)));
        RowSnapshot<Tasks> task3 = new VirtualRowSnapshot<>(new TaskListScoped(taskList2, new TasksTable(mAuthority)));

        assertThat(new MultiBatch(
                new Put<>(taskList1, new NameData("list1")),
                new Put<>(taskList2, new NameData("list2")),
                new Put<>(task1, new TitleData("task1")),
                new Put<>(task2, new TitleData("task2")),
                new Put<>(task3, new TitleData("task3"))

        ), resultsIn(mClient,
                new Assert<>(taskList1, new NameData("list1")),
                new Assert<>(taskList2, new NameData("list2")),
                new Assert<>(task1, new TitleData("task1")),
                new Assert<>(task2, new TitleData("task2")),
                new Assert<>(task3, new TitleData("task3")),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task1, Instances.TASK_ID),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task2, Instances.TASK_ID),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task3, Instances.TASK_ID)
        ));
    }


    @Test
    public void testInsertTaskWithStartAndDue()
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(new TaskListScoped(taskList, new TasksTable(mAuthority)));

        DateTime start = DateTime.now();
        DateTime due = start.addDuration(new Duration(1, 1, 0));

        assertThat(new MultiBatch(
                new Put<>(taskList, new EmptyRowData<TaskLists>()),
                new Put<>(task, new TimeData(start, due))

        ), resultsIn(mClient,
                new Assert<>(task, new TimeData(start, due)),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID, new AllOf(
                        new EqArg(Instances.INSTANCE_START, start.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DUE, due.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DURATION, due.getTimestamp() - start.getTimestamp()),
                        new EqArg(Tasks.TZ, start.isAllDay() ? "UTC" : start.getTimeZone().getID())
                ))
        ));
    }


    @Test
    public void testInsertWithStartAndDuration()
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(new TaskListScoped(taskList, new TasksTable(mAuthority)));

        DateTime start = DateTime.now();
        String durationStr = "PT1H";
        Duration duration = Duration.parse(durationStr);
        long durationMillis = duration.toMillis();
        TimeZone timeZone = TimeZone.getDefault();

        assertThat(new MultiBatch(
                new Put<>(taskList, new EmptyRowData<TaskLists>()),
                new Put<>(task, new TimeData(start, durationStr))

        ), resultsIn(mClient,
                new Assert<>(task, new TimeData(start, durationStr)),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID, new AllOf(
                        new EqArg(Instances.INSTANCE_START, start.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DUE, start.addDuration(duration).getTimestamp()),
                        new EqArg(Instances.INSTANCE_DURATION, durationMillis),
                        new EqArg(Tasks.TZ, "UTC")
                ))
        ));
    }


    /**
     * Having a table with a single task. Update task and check if instance updates accordingly.
     */
    @Test
    public void testUpdateDue()
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(new TaskListScoped(taskList, new TasksTable(mAuthority)));
        OperationsQueue queue = new BasicOperationsQueue(mClient);

        DateTime start = DateTime.now();
        DateTime due = start.addDuration(new Duration(1, 0, 1));
        TimeZone timeZone = TimeZone.getDefault();

        assertThat(new MultiBatch(
                new Put<>(taskList, new NameData("list1")),
                new Put<>(task, new TimeData(start, due))

        ), resultsIn(queue,
                new Assert<>(task, new TimeData(start, due)),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID, new AllOf(
                        new EqArg(Instances.INSTANCE_START, start.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DUE, due.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DURATION, due.getTimestamp() - start.getTimestamp()),
                        new EqArg(Tasks.TZ, "UTC")
                ))
        ));

        DateTime due2 = due.addDuration(new Duration(1, 0, 2));

        assertThat(new SingletonBatch(
                new Put<>(task, new TimeData(start, due2))

        ), resultsIn(queue,
                new Assert<>(task, new TimeData(start, due2)),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID, new AllOf(
                        new EqArg(Instances.INSTANCE_START, start.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DUE, due2.getTimestamp()),
                        new EqArg(Instances.INSTANCE_DURATION, due2.getTimestamp() - start.getTimestamp()),
                        new EqArg(Tasks.TZ, "UTC")
                ))
        ));
    }


    /**
     * Having a table with a single task. Delete task and check if instance is deleted accordingly.
     */
    @Test
    public void testInstanceDelete()
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        Table<Tasks> taskTable = new TaskListScoped(taskList, new TasksTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(taskTable);
        OperationsQueue queue = new BasicOperationsQueue(mClient);

        assertThat(new MultiBatch(
                new Put<>(taskList, new NameData("list1")),
                new Put<>(task, new TitleData("task1"))

        ), resultsIn(queue,
                new Assert<>(taskList, new NameData("list1")),
                new Assert<>(task, new TitleData("task1")),
                new PostOperationRelatedAssert<>(new InstanceTable(mAuthority), task, Instances.TASK_ID)
        ));

        assertThat(new SingletonBatch(
                new Delete<>(task)

        ), resultsIn(queue,
                new AssertEmptyTable<>(new TasksTable(mAuthority)),
                new AssertEmptyTable<>(new InstanceTable(mAuthority))
        ));
    }


    /**
     * LIST_IDs are required on creation.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithOutListId() throws Exception
    {
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(new TasksTable(mAuthority));
        OperationsQueue queue = new BasicOperationsQueue(mClient);
        queue.enqueue(new SingletonBatch(new Put<>(task, new TitleData("task1"))));
        queue.flush();
    }


    /**
     * LIST_IDs have to refer to an existing TaskList.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithInvalidId()
    {
        ContentValues values = new ContentValues();
        values.put(Tasks.LIST_ID, 5);
        mResolver.insert(Tasks.getContentUri(mAuthority), values);
    }


    @Test
    public void testExceptionalInstance_settingSyncId_shouldUpdateRegularId() throws Exception
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        Table<Tasks> taskTable = new TaskListScoped(taskList, new TasksTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(taskTable);
        RowSnapshot<Tasks> exceptionTask = new VirtualRowSnapshot<>(taskTable);

        OperationsQueue queue = new BasicOperationsQueue(mClient);

        queue.enqueue(new MultiBatch(
                new Put<>(taskList, new NameData("list1")),
                new Put<>(task, new Composite<>(
                        new TitleData("task1"),
                        new SyncIdData("syncId1"))
                )
        ));
        queue.flush();

        assertThat(new SingletonBatch(
                new Put<>(exceptionTask, new Composite<>(
                        new TitleData("task1exception"),
                        new OriginalInstanceSyncIdData("syncId1"))
                )

        ), resultsIn(queue,
                new PostOperationRelatedAssert<>(new TasksTable(mAuthority), task, Tasks.ORIGINAL_INSTANCE_ID,
                        new TitleData("task1exception"))
        ));
    }


    @Test
    public void testExceptionalInstance_settingRegularId_shouldUpdateSyncId() throws Exception
    {
        RowSnapshot<TaskLists> taskList = new VirtualRowSnapshot<>(new LocalTaskListsTable(mAuthority));
        Table<Tasks> taskTable = new TaskListScoped(taskList, new TasksTable(mAuthority));
        RowSnapshot<Tasks> task = new VirtualRowSnapshot<>(taskTable);
        RowSnapshot<Tasks> exceptionTask = new VirtualRowSnapshot<>(taskTable);

        OperationsQueue queue = new BasicOperationsQueue(mClient);

        queue.enqueue(new MultiBatch(
                new Put<>(taskList, new NameData("list1")),
                new Put<>(task, new Composite<>(
                        new TitleData("task1"),
                        new SyncIdData("syncId1"))
                )
        ));
        queue.flush();

        assertThat(new SingletonBatch(
                new Related<>(task, Tasks.ORIGINAL_INSTANCE_ID,
                        new Put<>(exceptionTask, new TitleData("task1exception")))

        ), resultsIn(queue,

                // TODO "Can't assert on a virtual row":
//                new Assert<>(exceptionTask, new Composite<>(
//                        new TitleData("task1exception"),
//                        new OriginalInstanceSyncIdData("syncId1")
//                )),

                new PostOperationRelatedAssert<>(new TasksTable(mAuthority), task, Tasks.ORIGINAL_INSTANCE_ID,
                        new Composite<>(
                                new TitleData("task1exception"),
                                new OriginalInstanceSyncIdData("syncId1")
                        ))
        ));
    }

}
