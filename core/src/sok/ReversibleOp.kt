package io.dwsoft.sok

sealed interface ReversibleOp<T> {
    interface NonSuspendingOld<T> : ReversibleOp<T>, () -> T {
        fun revert()

        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspendingOld<T> =
                object : NonSuspendingOld<T>, () -> T by op {
                    override fun revert() = rollback()
                }
        }
    }

    interface SuspendingOld<T> : ReversibleOp<T>, suspend () -> T {
        suspend fun revert()
        fun toNonSuspending(): NonSuspendingOld<T>
    }

    interface NonSuspending<T> : ReversibleOp<T>, ReversibleFunction<T> {
        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspending<T> =
                object : NonSuspending<T>, ReversibleFunction<T> {
                    override fun invoke(): ResultWithRollback<T> = op() to rollback
                }
        }
    }

    interface Suspending<T> : ReversibleOp<T>, SuspendingReversibleFunction<T> {
        fun toNonSuspending(): NonSuspending<T>
    }
}

typealias ReversibleFunction<T> = () -> ResultWithRollback<T>
typealias ResultWithRollback<T> = Pair<T, () -> Unit>

typealias SuspendingReversibleFunction<T> = suspend () -> ResultWithSuspendingRollback<T>
typealias ResultWithSuspendingRollback<T> = Pair<T, suspend () -> Unit>
