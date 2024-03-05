package io.dwsoft.sok

sealed interface ReversibleOp<T> {
    interface NonSuspending<T> : ReversibleOp<T>, ReversibleFunction<T> {
        companion object {
            operator fun <T> invoke(op: () -> T, rollback: () -> Unit): NonSuspending<T> =
                object : NonSuspending<T> {
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
