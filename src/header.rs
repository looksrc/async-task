use core::cell::UnsafeCell;
use core::fmt;
use core::task::Waker;

#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;
#[cfg(feature = "portable-atomic")]
use portable_atomic::AtomicUsize;

use crate::raw::TaskVTable;
use crate::state::*;
use crate::utils::abort_on_panic;

/// The header of a task.
/// 任务头数据。
///
/// This header is stored in memory at the beginning of the heap-allocated task.
/// 头数据存储在任务内存块的头部。
pub(crate) struct Header<M> {
    /// Current state of the task.
    /// 当前任务状态。
    ///
    /// Contains flags representing the current state and the reference count.
    /// 构成：
    /// - 最低字节：存储任务状态标记位
    /// - 高位字节：存储任务内存块的引用计数，引用计数每变化1，state值变化256(对应REFERENCE的值)
    pub(crate) state: AtomicUsize,

    /// The task that is blocked on the `Task` handle.
    /// 阻塞于Task句柄的任务唤醒器
    ///
    /// This waker needs to be woken up once the task completes or is closed.
    /// 当任务完成或关闭时利用它唤醒Task::await任务。
    pub(crate) awaiter: UnsafeCell<Option<Waker>>,

    /// The virtual table.
    /// 任务虚表。
    ///
    /// In addition to the actual waker virtual table, it also contains pointers to several other
    /// methods necessary for bookkeeping the heap-allocated task.
    /// 除了唤醒器携带的虚表以外，还需要一些通过任务虚表，
    pub(crate) vtable: &'static TaskVTable,

    /// Metadata associated with the task.
    /// 任务元数据。
    ///
    /// This metadata may be provided to the user.
    /// 提供给用户使用。
    pub(crate) metadata: M,

    /// Whether or not a panic that occurs in the task should be propagated.
    /// 任务逻辑Future中的恐慌是否传播给
    #[cfg(feature = "std")]
    pub(crate) propagate_panic: bool,
}

impl<M> Header<M> {
    /// Notifies the awaiter blocked on this task.
    /// 唤醒Task::await。
    ///
    /// If the awaiter is the same as the current waker, it will not be notified.
    /// 
    /// current：
    /// - 是在Task::poll中调用notify()时从轮询上下文cx中取出的唤醒器。
    /// 
    /// 当current=None：
    /// - 利用任务内置的Waker执行Task唤醒，这里时纯粹的唤醒，没其它意思。
    /// 
    /// 当current!=None：
    /// - 作用：校验当前任务内置的唤醒器是否指向其它Task句柄的。
    /// - 如果current与任务内置唤醒器相同时，什么都不干。
    /// - 如果current与任务内置唤醒器不同时，说明还有其它Task副本在等待，将内置的唤醒。
    #[inline]
    pub(crate) fn notify(&self, current: Option<&Waker>) {
        if let Some(w) = self.take(current) {
            abort_on_panic(|| w.wake());
        }
    }

    /// Takes the awaiter blocked on this task.
    /// 获取Task::await的内置唤醒器。
    ///
    /// If there is no awaiter or if it is the same as the current waker, returns `None`.
    /// 
    /// current=None：
    /// - 直接返回任务内置唤醒器
    /// 
    /// current!=None：
    /// - 作用：校验内置唤醒器和提供的唤醒器是否相同，如果不同则返回内置的。
    /// - 如果current与内置唤醒器相同，则丢弃内置唤醒器。返回None。
    /// - 如果current与内置唤醒器不同，则返回内置唤醒器。
    #[inline]
    pub(crate) fn take(&self, current: Option<&Waker>) -> Option<Waker> {
        // Set the bit indicating that the task is notifying its awaiter.
        // 利用NOTIFYING标记锁定从任务中取出唤醒器的操作。一旦被一个人取出其他人都取不到了。
        // 确保只有一个人能取出唤醒器，防止被多个线程同时取出，重复唤醒。
        let state = self.state.fetch_or(NOTIFYING, Ordering::AcqRel);

        // If the task was not notifying or registering an awaiter...
        // 检查是否被其它线程锁定，如果没有，则可以正常执行操作了。
        if state & (NOTIFYING | REGISTERING) == 0 {
            // Take the waker out.
            // 从任务中取出唤醒器。
            let waker = unsafe { (*self.awaiter.get()).take() };

            // Unset the bit indicating that the task is notifying its awaiter.
            // 解锁释放NOTIFYING标记。
            self.state
                .fetch_and(!NOTIFYING & !AWAITER, Ordering::Release);

            // Finally, notify the waker if it's different from the current waker.
            // 比对传入的唤醒器，确定返回的唤醒器。如果唤醒目标相同则销毁取出的唤醒器。
            if let Some(w) = waker {
                match current {
                    None => return Some(w),
                    Some(c) if !w.will_wake(c) => return Some(w),
                    Some(_) => abort_on_panic(|| drop(w)),
                }
            }
        }

        None
    }

    /// Registers a new awaiter blocked on this task.
    /// 注册一个Task等待者。
    ///
    /// This method is called when `Task` is polled and it has not yet completed.
    /// 当Task实例被轮询且尚未完成时注册一个唤醒器。
    /// 主要逻辑是，依据状态NOTIFY变化判定是否注册时直接执行唤醒。
    #[inline]
    pub(crate) fn register(&self, waker: &Waker) {
        // Load the state and synchronize with it.
        // 读取状态值。
        let mut state = self.state.fetch_or(0, Ordering::Acquire);

        // ① 唤醒器插入前：
        // 1.如果当前任务状态为通知，则直接执行唤醒，不插入唤醒器了。
        // 2.否则，利用乐观锁更改任务状态为REGISTERING，与其它线程的注册操作互斥。
        loop {
            // There can't be two concurrent registrations because `Task` can only be polled
            // by a unique pinned reference.
            // 调试：不能有多个线程同时执行注册操作。
            debug_assert!(state & REGISTERING == 0);

            // If we're in the notifying state at this moment, just wake and return without
            // registering.
            // 如果任务当前处于通知状态，直接执行唤醒并返回
            if state & NOTIFYING != 0 {
                abort_on_panic(|| waker.wake_by_ref());
                return;
            }

            // Mark the state to let other threads know we're registering a new awaiter.
            // 结合乐观锁，将状态标记更新为REGISTERING，用作和其它线程的注册操作互斥。
            match self.state.compare_exchange_weak(
                state,
                state | REGISTERING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    state |= REGISTERING;
                    break;
                }
                Err(s) => state = s,
            }
        }

        // Put the waker into the awaiter field.
        // ② 向任务中插入唤醒器。
        unsafe {
            abort_on_panic(|| (*self.awaiter.get()) = Some(waker.clone()));
        }

        // This variable will contain the newly registered waker if a notification comes in before
        // we complete registration.
        // 如果还没注册完成就收到了通知，则用waker临时存储唤醒器。
        let mut waker = None;

        // ③ 唤醒器插入后：
        // - 1.依据最新的NOTIFY状态，判定是否取出唤醒器，并写入最终确定的STATE值。
        // - 2.如果STATE变化后碰巧NOTIFY了，则唤醒器会被取出，程序末尾执行唤醒操作。
        // - 3.如果STATE没变化或者变化后没有NOTIFY，则更新完STATE状态后就返回。
        loop {
            // If there was a notification, take the waker out of the awaiter field.
            if state & NOTIFYING != 0 {
                if let Some(w) = unsafe { (*self.awaiter.get()).take() } {
                    abort_on_panic(|| waker = Some(w));
                }
            }

            // The new state is not being notified nor registered, but there might or might not be
            // an awaiter depending on whether there was a concurrent notification.
            // 构造新的任务状态：
            // 1.如果当前不是通知状态，则取不到waker，新状态设为非通知、非注册、等待。
            // 2.如果当前使通知状态，则取到了waker，新状态设为非通知、非注册、非等待。
            let new = if waker.is_none() {
                (state & !NOTIFYING & !REGISTERING) | AWAITER
            } else {
                state & !NOTIFYING & !REGISTERING & !AWAITER
            };

            // 利用乐观锁更新当前状态
            match self
                .state
                .compare_exchange_weak(state, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(s) => state = s,
            }
        }

        // If there was a notification during registration, wake the awaiter now.
        // 如果变为了通知状态则说明取出了waker，此时直接执行唤醒。
        if let Some(w) = waker {
            abort_on_panic(|| w.wake());
        }
    }
}

impl<M: fmt::Debug> fmt::Debug for Header<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::SeqCst);

        f.debug_struct("Header")
            .field("scheduled", &(state & SCHEDULED != 0))
            .field("running", &(state & RUNNING != 0))
            .field("completed", &(state & COMPLETED != 0))
            .field("closed", &(state & CLOSED != 0))
            .field("awaiter", &(state & AWAITER != 0))
            .field("task", &(state & TASK != 0))
            .field("ref_count", &(state / REFERENCE))
            .field("metadata", &self.metadata)
            .finish()
    }
}
