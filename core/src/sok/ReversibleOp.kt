package io.dwsoft.sok

sealed interface ReversibleOp<T> {
    interface NonSuspending<T> : ReversibleOp<T>, ReversibleFunction<T> {
        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspending<T> =
                object : NonSuspending<T> {
                    override fun invoke(): ResultWithRollback<T> = ResultWithRollback(op(), rollback)
                }
        }
    }

    interface Suspending<T> : ReversibleOp<T>, SuspendingReversibleFunction<T> {
        fun toNonSuspending(): NonSuspending<T>
    }
}

data class ResultWithRollback<T>(val result: T, val rollback: () -> Unit)

typealias ReversibleFunction<T> = () -> ResultWithRollback<T>

data class ResultWithSuspendingRollback<T>(val result: T, val rollback: () -> Unit)

typealias SuspendingReversibleFunction<T> = suspend () -> ResultWithSuspendingRollback<T>
