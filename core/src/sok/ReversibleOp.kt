package io.dwsoft.sok

sealed interface ReversibleOp<out T> {
    interface NonSuspending<out T> : ReversibleOp<T>, ReversibleFunction<T> {
        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspending<T> =
                object : NonSuspending<T> {
                    override fun invoke(): ResultWithRollback<T> = ResultWithRollback(op(), rollback)
                }
        }
    }

    interface Suspending<out T> : ReversibleOp<T>, SuspendingReversibleFunction<T> {
        fun toNonSuspending(): NonSuspending<T>
    }
}

data class ResultWithRollback<out T>(val result: T, val rollback: () -> Unit)

typealias ReversibleFunction<T> = () -> ResultWithRollback<T>

data class ResultWithSuspendingRollback<out T>(val result: T, val rollback: suspend () -> Unit)

typealias SuspendingReversibleFunction<T> = suspend () -> ResultWithSuspendingRollback<T>
