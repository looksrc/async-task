/// Set if the task is scheduled for running.
/// 设置任务是否被调度去执行。
///
/// A task is considered to be scheduled whenever its `Runnable` exists.
/// 不管任务的`Runnable`是否存在，不影响任务被调度。
///
/// This flag can't be set when the task is completed. However, it can be set while the task is
/// running, in which case it will be rescheduled as soon as polling finishes.
/// 任务完成后，需要清除此标记。
/// 任务运行中，如果有此标记，本次轮询完后立即会被重新调度。
pub(crate) const SCHEDULED: usize = 1 << 0;

/// Set if the task is running.
/// 设置任务是否正在轮询中(poll中)。
///
/// A task is in running state while its future is being polled.
///
/// This flag can't be set when the task is completed. However, it can be in scheduled state while
/// it is running, in which case it will be rescheduled as soon as polling finishes.
/// 任务完成后，需要清楚此标记。
/// 此标记可以与SCHEDULED标记并存，一旦本次轮询完后立即会被重新调度。
pub(crate) const RUNNING: usize = 1 << 1;

/// Set if the task has been completed.
/// 标记任务已完成。
///
/// This flag is set when polling returns `Poll::Ready`. The output of the future is then stored
/// inside the task until it becomes closed. In fact, `Task` picks up the output by marking
/// the task as closed.
/// 当任务轮询返回`Poll::Ready`后，设置此标记。
/// 然后，任务的输出会存在RawTask内存块中，一直到任务被关闭。
/// 实际，Task会将任务标记为关闭并拾取输出结果。
///
/// This flag can't be set when the task is scheduled or running.
/// 此标记与SCHEDULED和RUNNING互斥。
pub(crate) const COMPLETED: usize = 1 << 2;

/// Set if the task is closed.
/// 标记任务是否关闭。
///
/// If a task is closed, that means it's either canceled or its output has been consumed by the
/// `Task`. A task becomes closed in the following cases:
///
/// 1. It gets canceled by `Runnable::drop()`, `Task::drop()`, or `Task::cancel()`.
/// 2. Its output gets awaited by the `Task`.
/// 3. It panics while polling the future.
/// 4. It is completed and the `Task` gets dropped.
///
/// 任务关闭意味着，要么被取消，要么`Task`已经获取到了任务结果：
/// 
/// 1.任务被取消：`Runnable::drop()`, `Task::drop()`, or `Task::cancel()`
/// 2.任务结果被`Task`获取。
/// 3.轮询Future时发生恐慌
/// 4.任务已完成且`Task`已被遗弃。
pub(crate) const CLOSED: usize = 1 << 3;

/// Set if the `Task` still exists.
/// 标记是否Task句柄依然存活。
///
/// The `Task` is a special case in that it is only tracked by this flag, while all other
/// task references (`Runnable` and `Waker`s) are tracked by the reference count.
/// `Task`是一个任务的特殊句柄只依赖于此标记进行跟踪。其它的任务句柄都通过引用计数进行跟踪。
pub(crate) const TASK: usize = 1 << 4;

/// Set if the `Task` is awaiting the output.
/// 标记是否有任务句柄`Task`在等待任务输出。
///
/// This flag is set while there is a registered awaiter of type `Waker` inside the task. When the
/// task gets closed or completed, we need to wake the awaiter. This flag can be used as a fast
/// check that tells us if we need to wake anyone.
/// 仅当任务中存在一个注册的唤醒器时此标记才会设置。
/// 任务关闭或完成时需要唤醒等待者。
/// 此标记可以用做一个快速判断是否有等待被唤醒的`Task::await`。
pub(crate) const AWAITER: usize = 1 << 5;

/// Set if an awaiter is being registered.
/// 标记是否正在注册唤醒器的逻辑中。用于并发互斥。
///
/// This flag is set when `Task` is polled and we are registering a new awaiter.
/// 当`Task`首次被轮询时会注册唤醒器。
pub(crate) const REGISTERING: usize = 1 << 6;

/// Set if the awaiter is being notified.
/// 标记是否正在唤醒逻辑中。用于并发互斥。
///
/// This flag is set when notifying the awaiter. If an awaiter is concurrently registered and
/// notified, whichever side came first will take over the reposibility of resolving the race.
/// 当正在唤醒等待者时设置此标记。等待者被通知和被注册，两个事件不管哪一边先触发，先触发的都需要负责解决竞争问题。
pub(crate) const NOTIFYING: usize = 1 << 7;

/// A single reference.
/// 一个引用计数对应的状态值的跨度。
///
/// The lower bits in the state contain various flags representing the task state, while the upper
/// bits contain the reference count. The value of `REFERENCE` represents a single reference in the
/// total reference count.
/// 因为状态值最低字节用来存储状态了，其它高位才实际用于存储计数。因此计数每变化1，状态值变化REFERENCE=256。
///
/// Note that the reference counter only tracks the `Runnable` and `Waker`s. The `Task` is
/// tracked separately by the `TASK` flag.
/// 引用计数只用来跟踪`Runnable`和`Waker`两种任务句柄，而`Task`句柄由`TASK`标记进行跟踪。
pub(crate) const REFERENCE: usize = 1 << 8;
